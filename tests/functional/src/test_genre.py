import os
import pytest
from tests.functional.utils.json_read import load_json


c = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# @pytest.mark.asyncio
# async def test_create_schemas_genre(es_client):
#     with open(f'{c}/testdata/schemas_genre.json', 'r') as file:
#         f = json.load(file)
#
#     await es_client.index(index='genre', body=f)


@pytest.mark.asyncio
async def test_bulk(es_client):
    # Заполнение данных для теста
    data = await load_json(file='genre.json', index_name='genre', id='9d284e83-21f0-4073-aac0-4abee51193d8')

    assert await es_client.bulk(body=data, index='genre', refresh=True)

    data2 = await load_json(file='genre2.json', index_name='genre', id='c020dab2-e9bd-4758-95ca-dbe363462173')

    assert await es_client.bulk(body=data2, index='genre', refresh=True)


@pytest.mark.asyncio
async def test_search_genre(make_get_request):
    # Выполнение запроса
    response = await make_get_request('/genre/')

    print(response)
    # Проверка результата
    assert response.status == 200

    assert len(response.body) != 0


@pytest.mark.asyncio
async def test_search_detailed(make_get_request):
    # Выполнение запроса
    response = await make_get_request('/genre/9d284e83-21f0-4073-aac0-4abee51193d8')

    # Проверка результата
    assert response.status == 200

    assert response.body['id'] == '9d284e83-21f0-4073-aac0-4abee51193d8'

    assert response.body['name'] == 'Action'

    assert response.body['description'] is None
