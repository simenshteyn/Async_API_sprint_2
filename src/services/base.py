import json
from typing import Optional, Union
from abc import ABC, abstractmethod

from .redis_cache import RedisCache
from models.models import Film, Person, Genre


class BaseService(RedisCache):

    @property
    @abstractmethod
    def es_index(self) -> str:
        pass

    @staticmethod
    @abstractmethod
    def model(*args, **kwargs) -> Optional[Union[Film, Person, Genre]]:
        pass

    async def get_film(self,
                       key: str, query: dict = None, body: dict = None) -> Optional[Union[Film, Person, Genre]]:
        film = await self._get_film_sorted_from_cache(key=key)
        if not film:
            if not body:
                body = {'query': {"match_all": {}}}
            film = await self._get_film_by_search_from_elastic(query=query, body=body)
            if not film:
                return None
            await self._put_film_to_cache(key=key, film_list=film)
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
