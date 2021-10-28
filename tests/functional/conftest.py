import aioredis
import pytest
import asyncio
import aiohttp

from elasticsearch import AsyncElasticsearch

from functional.settings import config
from functional.utils.models import HTTPResponse


@pytest.fixture(scope="session")
def event_loop(request):
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='session')
async def redis_client():
    redis = await aioredis.create_redis_pool((f'{config.redis_host}',
                                              config.redis_port))

    await redis.flushall()
    yield redis
    redis.close()
    await redis.wait_closed()


@pytest.fixture(scope='session')
async def es_client():
    client = AsyncElasticsearch(hosts=f'{config.es_host}:{config.es_port}')
    yield client
    await client.close()


@pytest.fixture(scope='session')
async def session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest.fixture(scope='session')
def make_get_request(session):
    async def inner(method: str, params: dict = None) -> HTTPResponse:
        params = params or {}
        url = '{protocol}://{host}:{port}/api/v{api_version}/{method}'.format(
            protocol=config.service_protocol,
            host=config.service_host,
            port=config.service_port,
            api_version=config.service_api_version,
            method=method
        )
        async with session.get(url, params=params) as response:
            return HTTPResponse(
                body=await response.json(),
                headers=response.headers,
                status=response.status,
            )

    return inner
