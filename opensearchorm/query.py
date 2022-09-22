import abc
from datetime import date, datetime
from enum import Enum
import logging
from typing import Any, Callable, Dict, List, Tuple, Type, TypeVar, Union

from opensearchorm.model import BaseModel

Model = TypeVar('Model', bound=BaseModel)


class Expr(abc.ABC):
    @abc.abstractmethod
    def compile(self) -> dict:
        ...


class Contains(Expr):
    def __init__(self, field: str, values: list, min_match: int = 1):
        self.field = field
        self.values = values
        self.min_match = min_match

    def compile(self):
        return {
            'bool': {
                'should': [MatchPhrase(self.field, v).compile() for v in self.values],
                'minimum_should_match': self.min_match,
            }
        }


class Range(Expr):
    def __init__(self, field: str, interval: Tuple[Any, Any], *, left_open: bool = False, right_open: bool = False):
        self.field = field
        self.interval = interval
        self.left_open = left_open
        self.right_open = right_open

    def compile(self):
        left, right = self.interval
        if isinstance(left, (date, datetime)):
            left = left.isoformat()
        if isinstance(right, (date, datetime)):
            right = right.isoformat()

        range = {}
        if left is not None:
            op = 'gt' if self.left_open else 'gte'
            range[op] = left
        if right is not None:
            op = 'lt' if self.right_open else 'lte'
            range[op] = right

        return {
            'range': {
                self.field: range,
            }
        }


class MatchPhrase(Expr):
    def __init__(self, field: str, value: str):
        self.field = field
        self.value = value

    def compile(self):
        return {
            'match_phrase': {
                self.field: self.value,
            }
        }


class MatchPhrasePrefix(Expr):
    def __init__(self, field: str, value: str):
        self.field = field
        self.value = value

    def compile(self):
        return {
            'match_phrase_prefix': {
                self.field: self.value,
            }
        }


class Wildcard(Expr):
    def __init__(self, field: str, value: str):
        self.field = field
        self.value = value

    def compile(self):
        return {
            'wildcard': {
                self.field: self.value,
            }
        }


class RegExp(Expr):
    def __init__(self, field: str, value: str):
        self.field = field
        self.value = value

    def compile(self):
        return {
            'regexp': {
                self.field: self.value,
            }
        }


class Operator(Enum):
    PREFIX = '__prefix'
    REGEXP = '__regexp'
    CONTAINS = '__contains'
    GTE = '__gte'
    GT = '__gt'
    LTE = '__lte'
    LT = '__lt'


OPERATOR_FUNCTIONS: Dict[Operator, Callable[[str, Any], Expr]] = {
    Operator.CONTAINS: lambda field, value: Contains(field, value),
    Operator.PREFIX: lambda field, value: MatchPhrasePrefix(field, value),
    Operator.REGEXP: lambda field, value: RegExp(field, value),
    Operator.GTE: lambda field, value: Range(field, (value, None)),
    Operator.GT: lambda field, value: Range(field, (value, None), left_open=True),
    Operator.LTE: lambda field, value: Range(field, (None, value)),
    Operator.LT: lambda field, value: Range(field, (None, value), right_open=True),
}


class ModelQuery(Expr):
    def __init__(self, model_cls: Type[Model]):
        self.__model_cls = model_cls
        self.__filter: List[Expr] = []
        self.__exclude: List[Expr] = []
        self.__union: List[Expr] = []

    def compile(self):
        return {
            'bool': {
                'must_not': [e.compile() for e in self.__exclude],
                'should': [e.compile() for e in self.__union],
                'filter': [e.compile() for e in self.__filter],
                'minimum_should_match': 1 if self.__union else 0,
            }
        }

    @property
    def valid_fields(self):
        # todo @functools.cached_property, not supported in python3.7
        return set(self.__model_cls.default_fields())

    def check_valid_field(self, field: str):
        assert field in self.valid_fields, f'check field name: {field}'

    def parse_clause(self, raw_field: str, value) -> Expr:
        field = raw_field
        for op in Operator:
            suffix: str = op.value
            if raw_field.endswith(suffix):
                field, _ = raw_field.rsplit(suffix)
                logging.debug('parse field: %s, raw: %s', field, raw_field)
                self.check_valid_field(field)
                return OPERATOR_FUNCTIONS[op](field, value)

        self.check_valid_field(field)
        return MatchPhrase(field, value)

    def parse_clauses(self, **kwargs):
        clauses = []
        for k, v in kwargs.items():
            cond = self.parse_clause(k, v)
            clauses.append(cond)
        return clauses

    def filter(self, *args: Expr, **kwargs):
        conditions = self.parse_clauses(**kwargs)
        self.__filter.extend(args)
        self.__filter.extend(conditions)

        return self

    def union(self, *args: Expr, **kwargs):
        conditions = self.parse_clauses(**kwargs)
        self.__union.extend(args)
        self.__union.extend(conditions)

        return self

    def exclude(self, *args: Expr, **kwargs):
        conditions = self.parse_clauses(**kwargs)
        self.__exclude.extend(args)
        self.__exclude.extend(conditions)

        return self
