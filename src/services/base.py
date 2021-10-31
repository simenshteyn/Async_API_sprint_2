import json
from abc import abstractmethod

from pydantic.json import pydantic_encoder
from pydantic import parse_raw_as
from services.caching import Cacheable
from models.models import Film
from services.es_search import EsService


CACHE_EXPIRE = 60 * 5


class BaseService:

    @property
    @abstractmethod
    def es_index(self):
        pass

    @staticmethod
    @abstractmethod
    def model(*args, **kwargs):
        pass

    @staticmethod
    @abstractmethod
    def es_field(*args):
        pass

    def __init__(self, cache: Cacheable, elastic: EsService):
        self.cache = cache
        self.elastic = elastic

    async def get_request(self,
                          sort_field: str = None,
                          filter_genre: str = None,
                          page_number: str = None,
                          page_size: int = None,
                          q: str = None) -> list:

        if q:
            query = None
            key = f'{self.es_index}:{q}'
        else:
            query = await self._query_dict(
                sort=sort_field,
                filter_genre=filter_genre,
                page_number=page_number,
                page_size=page_size
            )
            key = self.es_index + ':' + ':'.join([str(b) for _, b in query.items()])

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

    async def _get_film_sorted_from_cache(self, key: str) -> str or list:
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
            q: str = None,) -> list:

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

    async def _query_dict(self,
                         sort: str = None,
                         filter_genre: str = None,
                         page_number: str = None,
                         page_size: int = None) -> dict:
        if self.model == Film:
            if not sort:
                sort_field = 'imdb_rating'
                sort_type = 'desc'
            else:
                sort_field = 'imdb_rating' if sort.endswith('imdb_rating') else None
                sort_type = 'desc' if sort.startswith('-') else 'asc'
        else:
            sort_field = None
            sort_type = None
        query = {
            'sort_field': sort_field,
            'sort_type': sort_type,
            'filter_genre': filter_genre,
            'page_number': page_number,
            'page_size': page_size
        }
        return query
