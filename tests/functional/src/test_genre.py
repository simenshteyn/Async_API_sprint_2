import pytest
from functional.utils.models import Genre
from functional.utils.extract import extract_payload


@pytest.fixture(scope='session')
async def load_testing_genre_data(es_client):
    payload = await extract_payload('search_genre.json', Genre, 'genre')
    await es_client.bulk(body=payload[0], index='genre', refresh=True)
    yield
    await es_client.bulk(body=payload[1], index='genre', refresh=True)


@pytest.mark.asyncio
async def test_search_genre(make_get_request, load_testing_genre_data, redis_client):
    # Выполнение запроса
    response = await make_get_request('genre/')

    # Проверка результата
    assert response.status == 200

    assert len(response.body) != 0

    data = await redis_client.get('genre:0:50')
    assert data


@pytest.mark.asyncio
async def test_search_detailed(make_get_request, redis_client):
    # Выполнение запроса
    response = await make_get_request('genre/some-test-id-aac0-4abee51193d8')

    # Проверка результата
    assert response.status == 200
    assert response.body['id'] == 'some-test-id-aac0-4abee51193d8'
    assert response.body['name'] == 'Action'
    assert response.body['description'] is None
    assert len(response.body) == 3

    data = await redis_client.get('some-test-id-aac0-4abee51193d8')
    assert data
    assert 'some-test-id-aac0-4abee51193d8' in data.decode('UTF-8').lower()
