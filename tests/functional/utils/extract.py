import json

from pydantic import BaseModel

from functional.utils.models import FilmShort, Film, Person, HTTPResponse


async def extract_films(response: HTTPResponse) -> list[FilmShort]:
    return [FilmShort.parse_obj(film) for film in response.body]


async def extract_film(response: HTTPResponse) -> Film:
    film = response.body
    return Film.parse_obj(film)


async def extract_people(response: HTTPResponse) -> list[Person]:
    return [Person.parse_obj(person) for person in response.body]


async def extract_person(response: HTTPResponse) -> Person:
    return Person.parse_obj(response.body)


async def extract_payload(file_name: str,
                          model: BaseModel,
                          es_index: str) -> tuple:
    """Extracts payload from json file for use in elastic bulk uploading. """
    file_path = f'testdata/{file_name}'
    with open(file_path) as json_file:
        data = json.load(json_file)
    obj_list = [model.parse_obj(some_obj) for some_obj in data]
    add_result = []
    for any_obj in obj_list:
        add_result.append(
            {"index": {"_index": es_index, "_id": any_obj.id}})
        add_result.append(any_obj.dict())
    add_payload = '\n'.join([json.dumps(line) for line in add_result]) + '\n'

    del_result = []
    for some_obj in obj_list:
        del_result.append({"delete": {"_index": es_index, "_id": some_obj.id}})
    del_payload = '\n'.join([json.dumps(line) for line in del_result]) + '\n'

    return add_payload, del_payload
