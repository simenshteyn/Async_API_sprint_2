import json
from typing import Optional, Union
from abc import abstractmethod

from pydantic.json import pydantic_encoder

from .caching import Cacheable
from models.models import Film, Person, Genre

from elasticsearch import AsyncElasticsearch


CACHE_EXPIRE = 60 * 5


class BaseService:

    @property
    @abstractmethod
    def es_index(self) -> str:
        pass

    @staticmethod
    @abstractmethod
    def model(*args, **kwargs) -> Optional[Union[Film, Person, Genre]]:
        pass

    def __init__(self, cache: Cacheable, elastic: AsyncElasticsearch):
        self.cache = cache
        self.elastic = elastic

    async def get_film(self,
                       key: str, query: dict = None, body: dict = None) -> Optional[Union[Film, Person, Genre]]:
        film = await self.cache.get(key=key)
        if not film:
            if not body:
                body = {'query': {"match_all": {}}}
            film = await self._get_film_by_search_from_elastic(query=query, body=body)
            if not film:
                return None
            await self.cache.set(key=key, value=json.dumps(film, default=pydantic_encoder), expire=CACHE_EXPIRE)
        return film

    async def _get_film_by_search_from_elastic(
            self, query: dict = None, body: dict = None) -> Optional[Union[Film, Person, Genre]]:

        doc = await self.elastic.search(
            index=self.es_index,
            body = body,
            size = query.get('page_size') if query else None,
            from_ = query.get('page_number') * query.get('page_size') if query else None,
            sort = f'{query.get("sort_field")}:{query.get("sort_type")}' if query else None,
        )
        result = []
        for movie in doc['hits']['hits']:
            result.append(self.model(**movie['_source']))
        return result
