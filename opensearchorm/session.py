import json
import logging
from typing import Generic, List, Union, Optional, Type, TypeVar, cast

from opensearchpy import OpenSearch

from opensearchorm.model import BaseModel
from opensearchorm.query import ModelQuery, Expr
from opensearchorm.aggs import Aggregation, Sum, Cardinality, Terms
from opensearchorm.utils import parse_aggregations

Host = Union[str, dict]
Model = TypeVar('Model', bound=BaseModel)


class SearchSession:
    def __init__(self, hosts: Union[Host, List[Host]], user: str, password: str, **kwargs) -> None:
        """
        :arg hosts: list of nodes, or a single node, we should connect to.
            Node should be a dictionary ({"host": "localhost", "port": 9200}),
            the entire dictionary will be passed to the :class:`~opensearchpy.Connection`
            class as kwargs, or a string in the format of ``host[:port]`` which will be
            translated to a dictionary automatically.

        :arg user: http auth username

        :arg password: http auth password

        :arg kwargs: any additional arguments will be passed on to the opensearch-py call
        """
        self.client = OpenSearch(
            hosts=hosts,
            http_auth=(user, password),
            http_compress=True,
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

    def scroll(self, scroll_id, lifetime):
        body = dict(
            scroll_id=scroll_id,
            scroll=lifetime,
        )
        return self.client.scroll(body=body)

    def count(self, **kwargs):
        return self.client.count(**kwargs)


class QueryExecutor(Generic[Model]):
    def __init__(self, model_cls: Type[Model], session: SearchSession):
        self.__query = ModelQuery(model_cls)
        self.__model_cls = model_cls
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

    def _search(self, fields: List[str], **kwargs):
        """
        :arg fields: include source fields

        :arg kwargs: any additional arguments will be passed on to the opensearch-py call
        """

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
            _source_includes=fields,
            **kwargs,
        )

        return resp

    def fetch_fields(self, fields: List[str], **kwargs):
        """
        :arg fields: include source fields

        :arg kwargs: any additional arguments will be passed on to the opensearch-py call
        """

        resp = self._search(fields, **kwargs)

        hits = resp['hits']['hits']
        logging.debug('raw result: %s', hits)
        return [hit['_source'] for hit in hits]

    def fetch(self, **kwargs):
        """
        :arg kwargs: any additional arguments will be passed on to the opensearch-py call
        """
        model = self.__model_cls
        hits = self.fetch_fields(model.default_fields(), **kwargs)
        return [model.parse_obj(hit) for hit in hits]

    def scroll(self, lifetime, **kwargs):
        model = self.__model_cls

        resp = self._search(model.default_fields(), scroll=lifetime, **kwargs)

        scroll_id = resp['_scroll_id']
        hits = resp['hits']['hits']
        logging.debug('raw result: %s', hits)
        hits = [hit['_source'] for hit in hits]
        data = [model.parse_obj(hit) for hit in hits]

        yield data

        while scroll_id and data:
            resp = self.__session.scroll(scroll_id, lifetime)
            scroll_id = resp['_scroll_id']
            hits = resp['hits']['hits']
            logging.debug('scroll raw result: %s', hits)
            hits = [hit['_source'] for hit in hits]
            data = [model.parse_obj(hit) for hit in hits]
            yield data

    def aggregate(self, aggs: Aggregation, **kwargs):
        """
        :arg kwargs: any additional arguments will be passed on to the opensearch-py call
        """

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

    def unique_count(self, field: str, **kwargs) -> int:
        resp = self.aggregate(Cardinality(field), **kwargs)
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

    def group_by(self, field: str, max_buckets: int = 100):
        return self.aggregate(Terms(field, max_buckets))
