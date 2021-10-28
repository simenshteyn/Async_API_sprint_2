import pytest

from functional.conftest import HTTPResponse, extract_payload
from functional.utils.models import FilmShort, Person


@pytest.fixture(scope='session')
async def load_testing_person_data(es_client):
    payload = await extract_payload('search_person.json', Person, 'person')
    await es_client.bulk(body=payload[0])
    yield
    await es_client.bulk(body=payload[1])


@pytest.fixture(scope='session')
async def load_testing_movies_data(es_client):
    payload = await extract_payload('search_movie.json', FilmShort, 'movies')
    await es_client.bulk(body=payload[0])
    yield
    await es_client.bulk(body=payload[1])


@pytest.mark.asyncio
async def test_search_movies(make_get_request, load_testing_movies_data, redis_client):
    response = await make_get_request(f'film/search/dog')
    assert response.status == 200
    assert len(response.body) != 0

    for i in response.body:
        assert 'dog' in i.get('title').lower()
    data = await redis_client.get('movies:dog')
    assert data
    assert 'dog' in data.decode('UTF-8').lower()


@pytest.mark.asyncio
async def test_search_person(make_get_request, load_testing_person_data, redis_client):
    response = await make_get_request('person/search/adam')

    assert response.status == 200
    assert len(response.body) != 0

    for i in response.body:
        assert 'adam' in i.get('full_name').lower()
    data = await redis_client.get('person:adam')
    assert data
    assert 'adam' in data.decode('UTF-8').lower()
