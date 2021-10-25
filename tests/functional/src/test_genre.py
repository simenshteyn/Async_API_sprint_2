import pytest
import json
from tests.functional.conftest import make_get_request
from tests.functional.utils.json_read import load_json


@pytest.mark.asyncio
async def test_bulk(es_client):
    # Заполнение данных для теста
    data = await load_json(index_name='genre', id='9d284e83-21f0-4073-aac0-4abee51193d8')
    await es_client.bulk(body=data, index='genre', refresh=True)


@pytest.mark.asyncio
async def test_search_detailed(make_get_request):
    # Выполнение запроса
    response = await make_get_request('/genre', {'search': 'Action'})

    # Проверка результата
    assert response.status == 200
    print(response.status)
    # assert len(response.body) == 1

    # assert response.body == expected