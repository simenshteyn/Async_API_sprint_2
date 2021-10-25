import os
import pytest
from tests.functional.utils.json_read import load_json

c = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope='session')
async def create_data_movies(es_client):
    # Заполнение данных для теста
    data = await load_json(file='search_movie.json', index_name='movies', id='273bd379-fdc8-4133-acc7-7be18ef1b699')

    await es_client.bulk(body=data, index='movies', refresh=True)

    data2 = await load_json(file='search_movie2.json', index_name='movies', id='d718c261-e954-4615-97f8-9ba67cd823a9')

    await es_client.bulk(body=data2, index='movies', refresh=True)


@pytest.fixture(scope='session')
async def create_data_person(es_client):
    data_person = await load_json(
        file='search_person.json', index_name='person', id='2cca941b-e05e-4314-9650-70155c752fa0'
    )

    await es_client.bulk(body=data_person, index='person', refresh=True)

    data_person2 = await load_json(
        file='search_person.json', index_name='person', id='2d6f6284-13ce-4d25-9453-c4335432c116'
    )

    await es_client.bulk(body=data_person2, index='person', refresh=True)


@pytest.mark.asyncio
async def test_search_movies(make_get_request, create_data_movies):
    # Выполнение запроса
    response = await make_get_request('/film/search/dog')

    # Проверка результата./
    assert response.status == 200

    assert len(response.body) != 0

    for i in response.body:
        assert 'dog' in i.get('title').lower()


@pytest.mark.asyncio
async def test_search_person(make_get_request, create_data_person):
    # Выполнение запроса
    response = await make_get_request('/person/search/adam')

    # Проверка результата./
    assert response.status == 200

    assert len(response.body) != 0

    for i in response.body:
        assert 'adam' in i.get('full_name').lower()
