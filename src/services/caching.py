from abc import ABC, abstractmethod
from typing import Union
import backoff
from aioredis import Redis


class Cacheable(ABC):
    @abstractmethod
    def get(self, key: str):
        pass

    @abstractmethod
    def set(self, key: str, value: str, expire: int):
        pass


class RedisService(Cacheable):
    def __init__(self, redis: Redis):
        self.redis = redis

    @backoff.on_exception(backoff.expo, Redis, max_time=10, factor=2)
    async def get(self, key: str) -> Union[str, bytes]:
        return await self.redis.get(key)

    @backoff.on_exception(backoff.expo, BaseException, max_time=10, factor=2)
    async def set(self, key: str, value: str, expire: int) -> None:
        await self.redis.set(key=key, value=value, expire=expire)
