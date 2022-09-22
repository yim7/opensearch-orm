# OpenSearch ORM
`opensearch-orm` is a high-level OpenSearch ORM for Python. The query syntax is similar to django-orm.

May be compatible with Elasticsearch, depending on opensearch-py.


# Installation
``` bash
pip install opensearchorm
```


# Getting Started

First, define your document model with indexing pattern.
``` python
from opensearchorm import SearchSession, BaseModel


class UserLog(BaseModel):
    __index__ = 'user_access_log-*'

    method: str
    path: str
    remote_ip: str
    created: datetime
```


You can use django-like syntax or typed query expressions together.
## filter
``` python
# {'bool': {'must_not': [], 'should': [], 'filter': [{'range': {'created': {'gte': '2022-09-01'}}}, {'match_phrase': {'remote_ip': '127.0.0.1'}}]}}        
with SearchSession() as session:
    result = (
        session.select(UserLog)
        .filter(created__gte='2022-09-01', remote_ip='127.0.0.1')
        .fetch()
    )
    print(result)

    # equals to
    result = (
        session.select(UserLog)
        .filter(Range('created', date(2022, 9, 1)), remote_ip='127.0.0.1')
        .fetch()
    )
```
## contains
``` python
# {'bool': {'must_not': [], 'should': [], 'filter': [{'bool': {'should': [{'match_phrase': {'method': 'GET'}}, {'match_phrase': {'method': 'POST'}}], 'minimum_should_match': 1}}]}}      
with SearchSession() as session:
    result = (
        session.select(UserLog)
        .filter(method__contains=['GET', 'POST'])
        .fetch()
    )
    print(result)

    # equals to
    result = (
        session.select(UserLog)
        .filter(Contains('method', ['GET', 'POST']))
        .fetch()
    )
```

## exclude
``` python
{'bool': {'must_not': [{'match_phrase': {'method': 'get'}}, {'match_phrase': {'path': '/login'}}], 'should': [], 'filter': []}}
with SearchSession() as session:
    result = (
        session.select(UserLog)
        .exclude(method='get', path='/login')
        .fetch()
    )
    print(result)
```


## paginate
``` python
with SearchSession() as session:
    result = (
        session.select(UserLog)
        .filter(method='get')
        .limit(100)
        .offset(100)
        .fetch()
    )
    print(result)
```

## aggregations
group by path and count unique remote_ip
``` python
with SearchSession() as session:
    result = (
        session.select(UserLog)
        .aggregate(Terms('path').nested(Cardinality('remote_ip')))
    )
    print(result)
    # result -> {'path': 1, 'path2': 2}
```