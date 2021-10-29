from functools import lru_cache

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import Film
from services.base import BaseService
from .caching import RedisService

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class FilmService(BaseService):
    es_index = 'movies'
    model = Film

    async def get_film_alike(self, film_id: str, key: str) -> list[Film] or None:
        film_list = await self._get_film_sorted_from_cache(key)
        if not film_list:
            body = {'query': {"match": {'_id': film_id}}}
            get_films = await self.get_film(key=film_id, body=body)
            film = get_films[0]
            film_list = []
            for genre in film.genre:
                query = {
                    'sort_field': 'imdb_rating',
                    'sort_type': 'desc',
                    'page_number': 0,
                    'page_size': 10
                }
                body = {"query": {"match": {"genre.id": {"query": genre['id']}}}}
                alike_films = await self._get_film_by_search_from_elastic(query=query, body=body)
                if alike_films:
                    film_list.extend(alike_films)
            await self._put_film_to_cache(key=key, film_list=list(film_list))

        return film_list


@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic)) -> FilmService:
    return FilmService(RedisService(redis), elastic)
