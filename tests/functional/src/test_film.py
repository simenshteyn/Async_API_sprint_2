import json
from pathlib import Path

import aioredis
import pytest
import aiohttp
import asyncio

from pydantic import BaseModel
from elasticsearch import AsyncElasticsearch
from http import HTTPStatus
from typing import Optional

from multidict import CIMultiDictProxy
from dataclasses import dataclass

from functional.settings import config


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


def extract_films(response: HTTPResponse) -> list[FilmShort]:
    return [FilmShort(
        id=film['id'],
        title=film['title'],
        imdb_rating=film['imdb_rating']) for film in response.body]


def extract_film(response: HTTPResponse) -> Film:
    film = response.body
    return Film(
        id=film['id'],
        imdb_rating=film['imdb_rating'],
        genre=film['genre'],
        title=film['title'],
        description=film['description'],
        director=film['director'],
        actors_names=film['actors_names'],
        writers_names=film['writers_names'],
        actors=film['actors'],
        writers=film['writers']
    )


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
async def load_testing_film_data(es_client):
    file_path = Path(__file__).parents[1] / 'testdata/test_films.json'
    with open(file_path) as json_file:
        data = json.load(json_file)
    film_list = [Film(
        id=film['id'],
        imdb_rating=film['imdb_rating'],
        genre=film['genre'],
        title=film['title'],
        description=film['description'],
        director=film['director'],
        actors_names=film['actors_names'],
        writers_names=film['writers_names'],
        actors=film['actors'],
        writers=film['writers']
    ) for film in data]
    add_result = []
    for film in film_list:
        add_result.append(
            {"index": {"_index": "movies", "_id": film.id}})
        add_result.append(film.dict())
    add_payload = '\n'.join([json.dumps(line) for line in add_result]) + '\n'
    await es_client.bulk(body=add_payload)

    yield

    del_result = []
    for film in film_list:
        del_result.append({"delete": {"_index": "movies", "_id": film.id}})
    del_payload = '\n'.join([json.dumps(line) for line in del_result]) + '\n'
    await es_client.bulk(body=del_payload)


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
async def test_general_film_list(make_get_request,
                                 load_testing_film_data,
                                 redis_client):
    response = await make_get_request('film/')
    films = extract_films(response)
    cache = await redis_client.get('imdb_rating:desc:None:movies:20:0')
    assert response.status == HTTPStatus.OK
    assert len(films) > 0
    assert cache


@pytest.mark.asyncio
async def test_film_search_via_uuid(make_get_request, redis_client):
    response_films = await make_get_request('film/')
    film_list = extract_films(response_films)
    film_uuid = film_list[0].id
    response = await make_get_request(f'film/{film_uuid}')
    film = extract_film(response)
    cache = await redis_client.get(f'{film_uuid}')
    assert response_films.status == HTTPStatus.OK
    assert response.status == HTTPStatus.OK
    assert film.id == film_uuid
    assert cache


@pytest.mark.asyncio
async def test_film_list_asc_sorting(make_get_request, redis_client):
    response = await make_get_request('film/?sort=imdb_rating')
    films = extract_films(response)
    cache = await redis_client.get('imdb_rating:asc:None:movies:20:0')
    assert response.status == HTTPStatus.OK
    assert len(films) > 1
    assert films[0].imdb_rating <= films[1].imdb_rating
    assert cache


@pytest.mark.asyncio
async def test_film_list_desc_sorting(make_get_request, redis_client):
    response = await make_get_request('film/?sort=-imdb_rating')
    films = extract_films(response)
    cache = await redis_client.get('imdb_rating:desc:None:movies:20:0')
    assert response.status == HTTPStatus.OK
    assert len(films) > 0
    assert films[0].imdb_rating >= films[1].imdb_rating
    assert cache


@pytest.mark.asyncio
async def test_film_list_page_number(make_get_request, redis_client):
    response = await make_get_request('film/?page_number=0')
    films = extract_films(response)
    cache = await redis_client.get('imdb_rating:desc:None:movies:20:0')
    assert response.status == HTTPStatus.OK
    assert len(films) > 0
    assert cache


@pytest.mark.asyncio
async def test_film_list_page_size(make_get_request, redis_client):
    response = await make_get_request('film/?page_size=10')
    films = extract_films(response)
    cache = await redis_client.get('imdb_rating:desc:None:movies:10:0')
    assert response.status == HTTPStatus.OK
    assert len(films) > 0
    assert cache


@pytest.mark.asyncio
async def test_film_list_page_number_and_size(make_get_request, redis_client):
    response = await make_get_request('film/?page_size=9&page_number=0')
    films = extract_films(response)
    cache = await redis_client.get('imdb_rating:desc:None:movies:9:0')
    assert response.status == HTTPStatus.OK
    assert len(films) > 0
    assert cache


@pytest.mark.asyncio
async def test_film_page_number_and_size_sorted_asc(make_get_request,
                                                    redis_client):
    response = await make_get_request('film/?page_size=8&page_number=1'
                                      '&sort=imdb_rating')
    films = extract_films(response)
    cache = await redis_client.get('imdb_rating:asc:None:movies:8:1')
    assert response.status == HTTPStatus.OK
    assert len(films) > 0
    assert films[0].imdb_rating <= films[1].imdb_rating
    assert cache


@pytest.mark.asyncio
async def test_film_page_number_and_size_sorted_desc(make_get_request,
                                                     redis_client):
    response = await make_get_request('film/?page_size=7&page_number=2'
                                      '&sort=-imdb_rating')
    films = extract_films(response)
    cache = await redis_client.get('imdb_rating:desc:None:movies:7:2')
    assert response.status == HTTPStatus.OK
    assert len(response.body) > 0
    assert films[0].imdb_rating >= films[1].imdb_rating
    assert cache


@pytest.mark.asyncio
async def test_film_search(make_get_request, redis_client):
    response_films = await make_get_request('film/')
    film_list = extract_films(response_films)
    film_title = film_list[0].title
    response = await make_get_request(f'film/search/{film_title}')
    search_films = extract_films(response)
    cache = await redis_client.get(f'movies:{film_title}')
    assert response.status == HTTPStatus.OK
    assert len(search_films) > 0
    assert cache


@pytest.mark.asyncio
async def test_film_popular_in_genre(make_get_request, redis_client):
    response = await make_get_request(
        'film/genre/3d8d9bf5-0d90-4353-88ba-4ccc5d2c07ff')
    films = extract_films(response)
    cache = await redis_client.get(
        f'imdb_rating:desc:3d8d9bf5-0d90-4353-88ba-4ccc5d2c07ff:movies:30:0')
    assert response.status == HTTPStatus.OK
    assert len(films) > 0
    assert cache


@pytest.mark.asyncio
async def test_film_alike(make_get_request, redis_client):
    response = await make_get_request('film/some-test-id-0d757c7e4f59/alike')
    films = extract_films(response)
    cache = await redis_client.get(f'alike:some-test-id-0d757c7e4f59')
    assert response.status == HTTPStatus.OK
    assert len(films) > 0
    assert cache


@pytest.mark.asyncio
async def test_es_uploading(make_get_request, redis_client):
    response = await make_get_request('film/some-test-id-0d757c7e4f59')
    film = extract_film(response)
    cache = await redis_client.get('some-test-id-0d757c7e4f59')
    assert film.id == "some-test-id-0d757c7e4f59"
    assert film.title == "Test Star Trek III: The Search for Spock"
    assert cache
