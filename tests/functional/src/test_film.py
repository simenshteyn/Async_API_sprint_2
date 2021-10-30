import pytest
from http import HTTPStatus

from functional.utils.models import FilmShort, Film
from functional.utils.extract import extract_films, extract_film, extract_payload


@pytest.fixture(scope='session')
async def load_testing_film_data(es_client):
    payload = await extract_payload('test_films.json', Film, 'movies')
    await es_client.bulk(body=payload[0], index='movies', refresh=True)
    yield
    await es_client.bulk(body=payload[1], index='movies', refresh=True)


@pytest.mark.asyncio
async def test_general_film_list(make_get_request,
                                 load_testing_film_data,
                                 redis_client):
    response = await make_get_request('film/')
    films = await extract_films(response)
    cache = await redis_client.get('movies:imdb_rating:desc:None:0:20')
    assert response.status == HTTPStatus.OK
    assert len(films) > 0
    assert cache


@pytest.mark.asyncio
async def test_film_search_via_uuid(make_get_request, redis_client):
    response_films = await make_get_request('film/')
    film_list = await extract_films(response_films)
    film_uuid = film_list[0].id
    response = await make_get_request(f'film/{film_uuid}')
    film = await extract_film(response)
    cache = await redis_client.get(f'movies:{film_uuid}')
    assert response_films.status == HTTPStatus.OK
    assert response.status == HTTPStatus.OK
    assert film.id == film_uuid
    assert cache


@pytest.mark.asyncio
async def test_film_list_asc_sorting(make_get_request, redis_client):
    response = await make_get_request('film/?sort=imdb_rating')
    films = await extract_films(response)
    cache = await redis_client.get('movies:imdb_rating:asc:None:0:20')
    assert response.status == HTTPStatus.OK
    assert len(films) > 1
    assert films[0].imdb_rating <= films[1].imdb_rating
    assert cache


@pytest.mark.asyncio
async def test_film_list_desc_sorting(make_get_request, redis_client):
    response = await make_get_request('film/?sort=-imdb_rating')
    films = await extract_films(response)
    cache = await redis_client.get('movies:imdb_rating:desc:None:0:20')
    assert response.status == HTTPStatus.OK
    assert len(films) > 0
    assert films[0].imdb_rating >= films[1].imdb_rating
    assert cache


@pytest.mark.asyncio
async def test_film_list_page_number(make_get_request, redis_client):
    response = await make_get_request('film/?page_number=0')
    films = await extract_films(response)
    cache = await redis_client.get('movies:imdb_rating:desc:None:0:20')
    assert response.status == HTTPStatus.OK
    assert len(films) > 0
    assert cache


@pytest.mark.asyncio
async def test_film_list_page_size(make_get_request, redis_client):
    response = await make_get_request('film/?page_size=10')
    films = await extract_films(response)
    cache = await redis_client.get('movies:imdb_rating:desc:None:0:10')
    assert response.status == HTTPStatus.OK
    assert len(films) > 0
    assert cache


@pytest.mark.asyncio
async def test_film_list_page_number_and_size(make_get_request, redis_client):
    response = await make_get_request('film/?page_size=9&page_number=0')
    films = await extract_films(response)
    cache = await redis_client.get('movies:imdb_rating:desc:None:0:9')
    assert response.status == HTTPStatus.OK
    assert len(films) > 0
    assert cache


@pytest.mark.asyncio
async def test_film_page_number_and_size_sorted_asc(make_get_request,
                                                    redis_client):
    response = await make_get_request('film/?page_size=8&page_number=1'
                                      '&sort=imdb_rating')
    films = await extract_films(response)
    cache = await redis_client.get('movies:imdb_rating:asc:None:1:8')
    assert response.status == HTTPStatus.OK
    assert len(films) > 0
    assert films[0].imdb_rating <= films[1].imdb_rating
    assert cache


@pytest.mark.asyncio
async def test_film_page_number_and_size_sorted_desc(make_get_request,
                                                     redis_client):
    response = await make_get_request('film/?page_size=7&page_number=2'
                                      '&sort=-imdb_rating')
    films = await extract_films(response)
    cache = await redis_client.get('movies:imdb_rating:desc:None:2:7')
    assert response.status == HTTPStatus.OK
    assert len(response.body) > 0
    assert films[0].imdb_rating >= films[1].imdb_rating
    assert cache


@pytest.mark.asyncio
async def test_film_search(make_get_request, redis_client):
    response_films = await make_get_request('film/')
    film_list = await extract_films(response_films)
    film_title = film_list[0].title
    response = await make_get_request(f'film/search/{film_title}')
    search_films = await extract_films(response)
    cache = await redis_client.get(f'movies:{film_title}')
    assert response.status == HTTPStatus.OK
    assert len(search_films) > 0
    assert cache


@pytest.mark.asyncio
async def test_film_popular_in_genre(make_get_request, redis_client):
    response = await make_get_request(
        'film/genre/3d8d9bf5-0d90-4353-88ba-4ccc5d2c07ff')
    films = await extract_films(response)
    cache = await redis_client.get(
        f'movies:3d8d9bf5-0d90-4353-88ba-4ccc5d2c07ff')
    assert response.status == HTTPStatus.OK
    assert len(films) > 0
    assert cache


@pytest.mark.asyncio
async def test_film_alike(make_get_request, redis_client):
    response = await make_get_request('film/some-test-id-0d757c7e4f59/alike')
    films = await extract_films(response)
    cache = await redis_client.get(f'alike:some-test-id-0d757c7e4f59')
    assert response.status == HTTPStatus.OK
    assert len(films) > 0
    assert cache


@pytest.mark.asyncio
async def test_es_uploading(make_get_request, redis_client):
    response = await make_get_request('film/some-test-id-0d757c7e4f59')
    film = await extract_film(response)
    cache = await redis_client.get('movies:some-test-id-0d757c7e4f59')
    assert film.id == "some-test-id-0d757c7e4f59"
    assert film.title == "Test Star Trek III: The Search for Spock"
    assert cache
