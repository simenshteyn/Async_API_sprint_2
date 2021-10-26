import pytest
import aiohttp
from elasticsearch import AsyncElasticsearch
from http import HTTPStatus

from multidict import CIMultiDictProxy
from dataclasses import dataclass

from functional.settings import config


@dataclass
class HTTPResponse:
    body: dict
    headers: CIMultiDictProxy[str]
    status: int


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


@pytest.fixture
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


@pytest.mark.asyncio
async def test_film_response_status(make_get_request):
    response = await make_get_request('film/')
    assert response.status == HTTPStatus.OK

@pytest.mark.asyncio
async def test_film_sorted_asc_status(make_get_request):
    response = await make_get_request('/film/?sort=-imdb_rating')
    assert response.status == HTTPStatus.OK