import json
from functools import lru_cache

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from pydantic.json import pydantic_encoder

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import Film
from services.base import BaseService
from .caching import RedisService
from .es_search import EsService


CACHE_EXPIRE = 60 * 5


class FilmService(BaseService):
    es_index = 'movies'
    model = Film
    es_field = ['id', 'title', 'genre.id']

    async def get_film_alike(self, film_id: str) -> list[Film] or None:
        film_list = await self._get_film_sorted_from_cache("alike:"+film_id)
        if not film_list:
            get_films = await self._get_film_by_search_from_elastic(
                query=None,
                q=film_id,
            )
            film = get_films[0]
            film_list = []
            query = {
                'sort_field': 'imdb_rating',
                'sort_type': 'desc',
                'page_number': 0,
                'page_size': 10
            }
            for genre in film.genre:
                alike_films = await self._get_film_by_search_from_elastic(
                    query,
                    q=genre['id']
                )
                print(genre)
                if alike_films:
                    film_list.extend(alike_films)
            await self.cache.set(
                key=f"alike:{film_id}",
                value=json.dumps(list(film_list),
                default=pydantic_encoder), expire=CACHE_EXPIRE)

        return film_list


@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic)) -> FilmService:
    return FilmService(RedisService(redis), EsService(elastic))
