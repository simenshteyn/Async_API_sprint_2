import json
from abc import ABC, abstractmethod

from aioredis import Redis
from elasticsearch import AsyncElasticsearch

from pydantic.json import pydantic_encoder
from pydantic import BaseModel, parse_raw_as
from typing import Optional, Union
from models.models import Film, Person, Genre


FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class RedisCache:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    @staticmethod
    @abstractmethod
    def model(*args, **kwargs) -> Optional[Union[Film, Person, Genre]]:
        pass

    async def _get_film_sorted_from_cache(self, key: str) -> Optional[Union[Film, Person, Genre]]:
        data = await self.redis.get(key)
        if not data:
            return None
        try:
            return parse_raw_as(list[self.model], data)
        except:
            return self.model.parse_raw(data)

    async def _put_film_to_cache(self, key: str, film_list: Optional[Union[Film, Person, Genre]]) -> None:
        await self.redis.set(key, json.dumps(film_list, default=pydantic_encoder), expire=FILM_CACHE_EXPIRE_IN_SECONDS)
