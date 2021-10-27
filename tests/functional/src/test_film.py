from typing import Optional

import pytest
import aiohttp
import asyncio
from elasticsearch import AsyncElasticsearch
from http import HTTPStatus

from multidict import CIMultiDictProxy
from dataclasses import dataclass

from functional.settings import config

from pydantic import BaseModel


class Film(BaseModel):
    id: str
    imdb_rating: float
    genre: Optional[list[dict[str, str]]] = None
    title: str
    description: Optional[str] = None
    director: Optional[list[dict[str, str]]] = None
    actors_names: Optional[list[str]] = None
    writers_names: Optional[list[str]] = None
    actors: Optional[list[dict[str, str]]] = None
    writers: Optional[list[dict[str, str]]] = None


class FilmShort(BaseModel):
    id: str
    title: str
    imdb_rating: Optional[float] = None


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
async def es_client():
    client = AsyncElasticsearch(hosts=f'{config.es_host}:{config.es_port}')
    yield client
    await client.close()


@pytest.fixture(scope='session')
async def load_test_data_to_es(es_client):
    data = data = await load_json(file='genre.json', index_name='genre',
                                  id='9d284e83-21f0-4073-aac0-4abee51193d8')
    await es_client.bulk(body=data, index='film', refresh=True)


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


@pytest.mark.asyncio
async def test_film_response_status(make_get_request):
    response = await make_get_request('film/')
    assert response.status == HTTPStatus.OK
    assert len(response.body) > 0


@pytest.mark.asyncio
async def test_film_sorted_asc_status(make_get_request):
    response = await make_get_request('film/?sort=imdb_rating')
    assert response.status == HTTPStatus.OK
    assert len(response.body) > 0


@pytest.mark.asyncio
async def test_film_sorted_desc_status(make_get_request):
    response = await make_get_request('film/?sort=-imdb_rating')
    assert response.status == HTTPStatus.OK
    assert len(response.body) > 0


@pytest.mark.asyncio
async def test_film_page_number(make_get_request):
    response = await make_get_request('film/?page_number=0')
    assert response.status == HTTPStatus.OK
    assert len(response.body) > 0


@pytest.mark.asyncio
async def test_film_page_size(make_get_request):
    response = await make_get_request('film/?page_size=10')
    assert response.status == HTTPStatus.OK
    assert len(response.body) > 0


@pytest.mark.asyncio
async def test_film_page_number_and_size(make_get_request):
    response = await make_get_request('film/?page_size=10&page_number=0')
    assert response.status == HTTPStatus.OK
    assert len(response.body) > 0


@pytest.mark.asyncio
async def test_film_page_number_and_size_sorted_asc(make_get_request):
    response = await make_get_request('film/?page_size=10&page_number=0'
                                      '&sort=imdb_rating')
    assert response.status == HTTPStatus.OK
    assert len(response.body) > 0


@pytest.mark.asyncio
async def test_film_page_number_and_size_sorted_desc(make_get_request):
    response = await make_get_request('film/?page_size=10&page_number=0'
                                      '&sort=-imdb_rating')
    assert response.status == HTTPStatus.OK
    assert len(response.body) > 0


@pytest.mark.asyncio
async def test_film_search(make_get_request):
    response = await make_get_request('film/search/dog')
    assert response.status == HTTPStatus.OK
    assert len(response.body) > 0


@pytest.mark.asyncio
@pytest.mark.xfail
async def test_film_popular_in_genre(make_get_request):
    response = await make_get_request('film/genre/foo')
    assert response.status == HTTPStatus.OK
    assert len(response.body) > 0
