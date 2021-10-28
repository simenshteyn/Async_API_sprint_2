import json
from datetime import date
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


class Person(BaseModel):
    id: str
    full_name: str
    birth_date: Optional[date] = None
    role: Optional[str] = None
    film_ids: list[str]


@dataclass
class HTTPResponse:
    body: dict
    headers: CIMultiDictProxy[str]
    status: int


def extract_people(response: HTTPResponse) -> list[Person]:
    return [Person(
        id=person['id'],
        full_name=person['full_name'],
        birth_date=person['birth_date'],
        role=person['role'],
        film_ids=person['film_ids']) for person in response.body]


def extract_person(response: HTTPResponse) -> Person:
    person = response.body
    return Person(
        id=person['id'],
        full_name=person['full_name'],
        birth_date=person['birth_date'],
        role=person['role'],
        film_ids=person['film_ids']
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
async def load_testing_person_data(es_client):
    file_path = Path(__file__).parents[1] / 'testdata/test_person.json'
    with open(file_path) as json_file:
        data = json.load(json_file)
    person_list = [Person(
        id=person['id'],
        full_name=person['full_name'],
        birth_date=person['birth_date'],
        role=person['role'],
        film_ids=person['film_ids']
    ) for person in data]
    add_result = []
    for person in person_list:
        add_result.append(
            {"index": {"_index": "person", "_id": person.id}})
        add_result.append(person.dict())
    add_payload = '\n'.join([json.dumps(line) for line in add_result]) + '\n'
    await es_client.bulk(body=add_payload)

    yield

    del_result = []
    for person in person_list:
        del_result.append({"delete": {"_index": "person", "_id": person.id}})
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
async def test_general_person_list(make_get_request,
                                   load_testing_person_data,
                                   redis_client):
    response = await make_get_request('person/')
    people = extract_people(response)
    cache = await redis_client.get('person:0:20')
    assert response.status == HTTPStatus.OK
    assert len(people) > 0
    assert cache


@pytest.mark.asyncio
async def test_person_search_via_uuid(make_get_request, redis_client):
    response_people = await make_get_request('person/')
    person_list = extract_people(response_people)
    person_uuid = person_list[0].id
    response = await make_get_request(f'person/{person_uuid}')
    person = extract_person(response)
    cache = await redis_client.get(f'{person_uuid}')
    assert response_people.status == HTTPStatus.OK
    assert response.status == HTTPStatus.OK
    assert person.id == person_uuid
    assert cache


@pytest.mark.asyncio
async def test_person_list_page_number(make_get_request, redis_client):
    response = await make_get_request('person/?page_number=0')
    people = extract_people(response)
    cache = await redis_client.get('person:0:20')
    assert response.status == HTTPStatus.OK
    assert len(people) > 0
    assert cache


@pytest.mark.asyncio
async def test_person_list_page_size(make_get_request, redis_client):
    response = await make_get_request('person/?page_size=9')
    people = extract_people(response)
    cache = await redis_client.get('person:0:9')
    assert response.status == HTTPStatus.OK
    assert (len(people) > 0) and (len(people) < 10)
    assert cache


@pytest.mark.asyncio
async def test_person_list_page_number_and_size(make_get_request, redis_client):
    response = await make_get_request('person/?page_size=8&page_number=0')
    people = extract_people(response)
    cache = await redis_client.get('person:0:8')
    assert response.status == HTTPStatus.OK
    assert (len(people) > 0) and (len(people) < 9)
    assert cache
