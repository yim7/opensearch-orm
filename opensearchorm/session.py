import logging
from typing import List, Optional, Type, TypeVar

from opensearchpy import OpenSearch

from opensearchorm.model import BaseModel
from opensearchorm.query import ModelQuery, Expr

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


class QueryExecutor:
    def __init__(self, model_cls: Type[Model], session):
        self.__query = ModelQuery(model_cls)
        self.__model_cls = model_cls
        self._include_fields = []
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._session = session

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
        self._limit = limit
        return self

    def offset(self, offset: int):
        self._offset = offset
        return self

    def values(self, fields: List[str]):
        self._include_fields = fields
        return self

    def fetch(self):
        body = {
            'query': self.__query.compile(),
        }

        logging.debug('query:\n%s', body)
        params = {
            'format': 'json',
            'request_timeout': 300,
        }

        model = self.__model_cls
        assert model and model.__index__, 'model has no index'

        data = self._session.search(
            body=body,
            params=params,
            index=model.__index__,
            size=self._limit,
            from_=self._offset,
            _source_includes=self._include_fields or model.default_fields(),
        )

        hits = data['hits']['hits']
        logging.debug('raw result: %s', hits)
        if self._include_fields:
            return [hit['_source'] for hit in hits]
        else:
            return [model.parse_obj(hit['_source']) for hit in hits]
