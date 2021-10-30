from functools import lru_cache

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import Person
from services.base import BaseService
from .caching import RedisService
from .es_search import EsService


class PersonService(BaseService):
    es_index = 'person'
    model = Person
    es_field = ['id', 'full_name']


@lru_cache()
def get_person_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(RedisService(redis), EsService(elastic))
