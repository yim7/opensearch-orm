import json
import logging
from typing import List, Optional, Type, TypeVar, cast

from opensearchpy import OpenSearch

from opensearchorm.model import BaseModel
from opensearchorm.query import ModelQuery, Expr
from opensearchorm.aggs import Aggregation, Sum, Cardinality

Model = TypeVar('Model', bound=BaseModel)


class SearchSession:
    def __init__(self, host: str, user: str, password: str, **kwargs) -> None:
        self.client = OpenSearch(
            hosts=[
                host,
            ],
            http_auth=(user, password),
            http_compress=True,
            use_ssl=True,
            verify_certs=True,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
            **kwargs,
        )

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.client.close()

    def select(self, model: Type[Model]):
        return QueryExecutor(model, self)

    def search(self, **kwargs):
        return self.client.search(**kwargs)

    def count(self, **kwargs):
        return self.client.count(**kwargs)


class QueryExecutor:
    def __init__(self, model_cls: Type[Model], session: SearchSession):
        self.__query = ModelQuery(model_cls)
        self.__model_cls = model_cls
        self.__include_fields = []
        self.__limit: Optional[int] = None
        self.__offset: Optional[int] = None
        self.__session = session

    def filter(self, *args: Expr, **kwargs):
        self.__query.filter(*args, **kwargs)
        return self

    def union(self, *args: Expr, **kwargs):
        self.__query.union(*args, **kwargs)
        return self

    def exclude(self, *args: Expr, **kwargs):
        self.__query.exclude(*args, **kwargs)
        return self

    def limit(self, limit: int):
        self.__limit = limit
        return self

    def offset(self, offset: int):
        self.__offset = offset
        return self

    def values(self, fields: List[str]):
        self.__include_fields = fields
        return self

    def fetch(self, **kwargs):
        body = {
            'query': self.__query.compile(),
        }
        logging.debug('query:\n%s', json.dumps(body))

        model = self.__model_cls
        assert model and model.__index__, 'model has no index'

        resp = self.__session.search(
            body=body,
            index=model.__index__,
            size=self.__limit,
            from_=self.__offset,
            _source_includes=self.__include_fields or model.default_fields(),
            **kwargs,
        )

        hits = resp['hits']['hits']
        logging.debug('raw result: %s', hits)
        if self.__include_fields:
            return [hit['_source'] for hit in hits]
        else:
            return [model.parse_obj(hit['_source']) for hit in hits]

    def scroll(self, **kwargs):
        ...

    def unique_count(self, field: str, is_text: bool = False, **kwargs) -> int:
        resp = self.aggregate(Cardinality(field, is_text), **kwargs)
        return cast(int, resp)

    def sum(self, field: str, **kwargs) -> float:
        resp = self.aggregate(Sum(field), **kwargs)
        return cast(int, resp)

    def count(self, **kwargs) -> int:
        body = {
            'query': self.__query.compile(),
        }
        logging.debug('query:\n%s', json.dumps(body))

        model = self.__model_cls
        assert model and model.__index__, 'model has no index'

        resp = self.__session.count(
            body=body,
            index=model.__index__,
            **kwargs,
        )
        return resp['count']

    def aggregate(self, aggs: Aggregation, **kwargs):
        body = {
            'query': self.__query.compile(),
            'aggs': aggs.compile(depth=1),
        }
        logging.debug('query:\n%s', json.dumps(body))

        model = self.__model_cls
        assert model and model.__index__, 'model has no index'

        resp = self.__session.search(
            body=body,
            index=model.__index__,
            size=0,
            **kwargs,
        )

        data = resp['aggregations']
        return parse_aggregations(data, depth=1)


def parse_aggregations(data: dict, depth: int = 1):
    level = data.get(str(depth), None)
    if level is None:
        return

    if 'buckets' in level:
        result = {}
        buckets = level['buckets']
        for b in buckets:
            key = b['key']
            count = b['doc_count']
            children = parse_aggregations(b, depth + 1)
            result[key] = children if children else count
        return result
    else:
        value = level['value']
        return value
