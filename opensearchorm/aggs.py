import abc
from typing import Optional


class Aggregation(abc.ABC):
    def __init__(self, field: str) -> None:
        self.field = field

    @abc.abstractmethod
    def compile(self, depth: int = 1):
        ...


class MetricAggregation(Aggregation):
    ...


class BucketAggregation(Aggregation):
    @abc.abstractmethod
    def nested(self, child: Aggregation):
        ...


class Terms(BucketAggregation):
    def __init__(self, field: str, max_buckets: int = 100) -> None:
        super().__init__(field)
        self.max_buckets = max_buckets
        self.child: Optional[Aggregation] = None

    def compile(self, depth: int = 1):
        return {
            depth: {
                'terms': {
                    'field': self.field,
                    'size': self.max_buckets,
                },
                'aggs': self.child.compile(depth + 1) if self.child else {},
            }
        }

    def nested(self, child: Aggregation):
        self.child = child
        return self


class Cardinality(MetricAggregation):
    def compile(self, depth: int = 1):
        return {
            depth: {
                'cardinality': {
                    'field': self.field,
                }
            }
        }


class Sum(MetricAggregation):
    def compile(self, depth: int = 1):
        return {
            depth: {
                'sum': {
                    'field': self.field,
                }
            }
        }
