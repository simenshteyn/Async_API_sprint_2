from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException

from models.models import Person
from services.person import PersonService, get_person_service

router = APIRouter()


@router.get('/{person_id}', response_model=Person,
            response_model_exclude_unset=True)
async def person_details(person_id: str,
                         person_service: PersonService = Depends(
                             get_person_service)) -> Person:
    body = {'query': {"match": {'_id': person_id}}}
    person = await person_service.get_request(key=person_id, body=body)
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='person not found')
    person = person[0]
    return Person(id=person.id,
                  full_name=person.full_name,
                  birth_date=person.birth_date,
                  role=person.role,
                  film_ids=person.film_ids)


@router.get('/', response_model=list[Person],
            response_model_exclude_unset=True)
async def person_list(
        page_number: int = 0,
        page_size: int = 20,
        person_service: PersonService = Depends(get_person_service)) -> list[
    Person]:
    query = {
         'page_number': page_number,
         'page_size': page_size
    }
    key = f'{"person"}:{page_number}:{page_size}'
    # key = ''.join(['person' + str(b) for i, b in query.items()])
    person_list = await person_service.get_request(key=key, query=query)
    if not person_list:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='persons not found')
    return [Person(id=person.id,
                   full_name=person.full_name,
                   birth_date=person.birth_date,
                   role=person.role,
                   film_ids=person.film_ids) for person in person_list]


@router.get('/search/{person_search_string}', response_model=list[Person],
            response_model_exclude_unset=True)
async def films_search(person_search_string: str,
                       person_service: PersonService = Depends(
                           get_person_service)) -> list[Person]:
    body = {"query": {
                "match": {
                    "full_name": {
                        "query": person_search_string,
                        "fuzziness": "auto"
                    }
                }
            }}
    person_list = await person_service.get_request(key=f'person:{person_search_string}', body=body)

    if not person_list:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='person not found')
    return [Person(id=person.id,
                   full_name=person.full_name,
                   birth_date=person.birth_date,
                   role=person.role,
                   film_ids=person.film_ids) for person in person_list]
