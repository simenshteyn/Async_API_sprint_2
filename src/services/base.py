import json
from typing import Optional, Union
from abc import abstractmethod

from pydantic.json import pydantic_encoder
from pydantic import parse_raw_as
from .caching import Cacheable
from models.models import Film, Person, Genre
from .es_search import EsService


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

    @staticmethod
    @abstractmethod
    def es_field(*args) -> list:
        pass

    def __init__(self, cache: Cacheable, elastic: EsService):
        self.cache = cache
        self.elastic = elastic

    async def get_request(self,
                          key: str,
                          query: dict = None,
                          q: str = None) -> Union:
        film = await self._get_film_sorted_from_cache(key=key)
        if not film:
            film = await self._get_film_by_search_from_elastic(
                query=query,
                q=q,
            )
            if not film:
                return None
            await self.cache.set(key=key, value=json.dumps(film, default=pydantic_encoder), expire=CACHE_EXPIRE)
        return film

    async def _get_film_sorted_from_cache(self, key: str) -> Optional[Union[Film, Person, Genre]]:
        data = await self.cache.get(key)
        if not data:
            return None
        try:
            return parse_raw_as(list[self.model], data)
        except:
            return self.model.parse_raw(data)

    async def _get_film_by_search_from_elastic(
            self,
            query: dict = None,
            q: str = None,) -> Optional[Union[Film, Person, Genre]]:

        doc = await self.elastic.get_search(
            es_index=self.es_index,
            func_name='body_search' if q else None,
            field=self.es_field,
            q=q,
            query=query,
        )
        result = []
        for movie in doc['hits']['hits']:
            result.append(self.model(**movie['_source']))
        return result
