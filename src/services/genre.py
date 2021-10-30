from functools import lru_cache

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import Genre
from services.base import BaseService
from .caching import RedisService
from .es_search import EsService


class GenreService(BaseService):
    es_index = 'genre'
    model = Genre
    es_field = ['id', 'name']


@lru_cache()
def get_genre_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(RedisService(redis), EsService(elastic))
