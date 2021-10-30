from typing import Union
from abc import ABC, abstractmethod
from elasticsearch import AsyncElasticsearch, ConnectionError
import backoff


class EsSearch(ABC):

    @abstractmethod
    def body_search(self, key: str, query: str):
        pass

    @abstractmethod
    def body_all(self):
        pass

    @abstractmethod
    def get_search(self,
                   es_index: str,
                   func_name: str,
                   field: list,
                   q: str = None,
                   query: dict = None):
        pass


class EsService(EsSearch):

    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def body_search(self, q: str, field: list):
        return {"query": {
            "multi_match": {
                "query": f"{q}",
                "fields": field
            }
        }}

    async def body_all(self):
        return {"query": {"match_all": {}}}

    @backoff.on_exception(backoff.expo, ConnectionError, max_time=10, factor=2)
    async def get_search(
            self,
            es_index: str,
            func_name: str,
            field: list,
            q: str = None,
            query: dict = None
    ) -> list:

        if q:
            body = await self.body_search(field=field, q=q)
        else:
            body = await self.body_all()

        doc = self.elastic.search(
            index=es_index,
            body = body,
            size = query.get('page_size') if query else None,
            from_ = query.get('page_number') * query.get('page_size') if query else None,
            sort = f'{query.get("sort_field")}:{query.get("sort_type")}' if query else None,
        )
        return await doc
