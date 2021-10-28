from functional.conftest import HTTPResponse, extract_payload
from functional.utils.models import FilmShort, Film, Person


def extract_films(response: HTTPResponse) -> list[FilmShort]:
    return [FilmShort.parse_obj(film) for film in response.body]


def extract_film(response: HTTPResponse) -> Film:
    film = response.body
    return Film.parse_obj(film)


def extract_people(response: HTTPResponse) -> list[Person]:
    return [Person.parse_obj(person) for person in response.body]


def extract_person(response: HTTPResponse) -> Person:
    return Person.parse_obj(response.body)
