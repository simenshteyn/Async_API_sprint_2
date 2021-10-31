from functools import lru_cache

from fastapi import Depends

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import Genre
from services.base import BaseService
from services.caching import RedisService
from services.es_search import EsService
from services.interfaces import Cacheable, EsSearch


class GenreService(BaseService):
    es_index = 'genre'
    model = Genre
    es_field = ['id', 'name']


@lru_cache()
def get_genre_service(
        redis: Cacheable = Depends(get_redis),
        elastic: EsSearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(RedisService(redis), EsService(elastic))
