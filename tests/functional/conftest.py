import json
import aioredis
import pytest
import asyncio
import aiohttp

from pydantic import BaseModel
from dataclasses import dataclass
from elasticsearch import AsyncElasticsearch
from multidict import CIMultiDictProxy

from functional.settings import config


@dataclass
class HTTPResponse:
    body: dict
    headers: CIMultiDictProxy[str]
    status: int


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


async def extract_payload(file_name: str,
                          model: BaseModel,
                          es_index: str) -> tuple:
    """Extracts payload from json file for use in elastic bulk uploading. """
    file_path = f'testdata/{file_name}'
    with open(file_path) as json_file:
        data = json.load(json_file)
    obj_list = [model.parse_obj(some_obj) for some_obj in data]
    add_result = []
    for any_obj in obj_list:
        add_result.append(
            {"index": {"_index": es_index, "_id": any_obj.id}})
        add_result.append(any_obj.dict())
    add_payload = '\n'.join([json.dumps(line) for line in add_result]) + '\n'

    del_result = []
    for some_obj in obj_list:
        del_result.append({"delete": {"_index": es_index, "_id": some_obj.id}})
    del_payload = '\n'.join([json.dumps(line) for line in del_result]) + '\n'

    return add_payload, del_payload
