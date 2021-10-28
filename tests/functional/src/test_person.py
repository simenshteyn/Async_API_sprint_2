import pytest

from pydantic import BaseModel
from http import HTTPStatus
from typing import Optional
from datetime import date

from functional.conftest import HTTPResponse, extract_payload


class Person(BaseModel):
    id: str
    full_name: str
    birth_date: Optional[date] = None
    role: Optional[str] = None
    film_ids: list[str]


def extract_people(response: HTTPResponse) -> list[Person]:
    return [Person.parse_obj(person) for person in response.body]


def extract_person(response: HTTPResponse) -> Person:
    return Person.parse_obj(response.body)


@pytest.fixture(scope='session')
async def load_testing_person_data(es_client):
    payload = await extract_payload('test_person.json', Person, 'person')
    await es_client.bulk(body=payload[0])
    yield
    await es_client.bulk(body=payload[1])


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
async def test_person_list_page_number_and_size(make_get_request,
                                                redis_client):
    response = await make_get_request('person/?page_size=8&page_number=0')
    people = extract_people(response)
    cache = await redis_client.get('person:0:8')
    assert response.status == HTTPStatus.OK
    assert (len(people) > 0) and (len(people) < 9)
    assert cache


@pytest.mark.asyncio
async def test_person_search(make_get_request, redis_client):
    response_people = await make_get_request('person/')
    person_list = extract_people(response_people)
    person_name = person_list[0].full_name
    response = await make_get_request(f'person/search/{person_name}')
    search_people = extract_people(response)
    cache = await redis_client.get(f'person:{person_name}')
    assert response.status == HTTPStatus.OK
    assert len(search_people) > 0
    assert cache


@pytest.mark.asyncio
async def test_es_person_uploading(make_get_request, redis_client):
    response = await make_get_request(
        'person/test-person-b55c-45f6-9200-41f153a72a7a')
    person = extract_person(response)
    cache = await redis_client.get('test-person-b55c-45f6-9200-41f153a72a7a')
    assert person.id == "test-person-b55c-45f6-9200-41f153a72a7a"
    assert person.full_name == "Test Jonathan Knight"
    assert cache
