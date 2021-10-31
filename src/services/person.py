from functools import lru_cache

from fastapi import Depends

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import Person
from services.base import BaseService
from services.caching import RedisService
from services.es_search import EsService
from services.interfaces import Cacheable, EsSearch


class PersonService(BaseService):
    es_index = 'person'
    model = Person
    es_field = ['id', 'full_name']


@lru_cache()
def get_person_service(
        redis: Cacheable = Depends(get_redis),
        elastic: EsSearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(RedisService(redis), EsService(elastic))
