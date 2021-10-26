import json
from typing import Optional, Union

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from pydantic import BaseModel, parse_raw_as
from pydantic.json import pydantic_encoder
from .redis_cache import RedisCache
from abc import ABC, abstractmethod
from models.models import Film, Person, Genre


class BaseService(RedisCache):

    @property
    @abstractmethod
    def es_index(self) -> str:
        pass

    @staticmethod
    @abstractmethod
    def model(*args, **kwargs) -> Optional[Union[Film, Person, Genre]]:
        pass

    async def get_film(self,
                       key: str, query: dict = None, body: dict = None) -> Optional[Union[Film, Person, Genre]]:
        film = await self._get_film_sorted_from_cache(key=key)
        if not film:
            if not body:
                body = {'query': {"match_all": {}}}
            film = await self._get_film_by_search_from_elastic(query=query, body=body)
            if not film:
                return None
            await self._put_film_to_cache(key=key, film_list=film)
        return film

    async def _get_film_by_search_from_elastic(
            self, query: dict = None, body: dict = None) -> Optional[Union[Film, Person, Genre]]:

        doc = await self.elastic.search(
            index=self.es_index,
            body = body,
            size = query.get('page_size') if query else None,
            from_ = query.get('page_number') * query.get('page_size') if query else None,
            sort = f'{query.get("sort_field")}:{query.get("sort_type")}' if query else None,
        )
        result = []
        for movie in doc['hits']['hits']:
            print(movie)
            result.append(self.model(**movie['_source']))
        return result




































    #
    # async def _get_by_id(
    #         self, id: str, cache_expire: int, model: BaseModel, es_index: str
    # ) -> Optional[BaseModel]:
    #     some_obj = await self._get_by_id_from_cache(id, model)
    #     if not some_obj:
    #         some_obj = await self._get_by_id_from_elastic(id, model, es_index)
    #         if not some_obj:
    #             return None
    #         await self._put_by_id_to_cache(some_obj, cache_expire)
    #     return some_obj
    #
    # async def _get_by_id_from_cache(
    #         self, id: str, model: BaseModel) -> Optional[BaseModel]:
    #     data = await self.redis.get(id)
    #     if not data:
    #         return None
    #     return model.parse_raw(data)
    #
    # async def _get_by_id_from_elastic(
    #         self, id: str, model: BaseModel, es_index: str) -> BaseModel:
    #     doc = await self.elastic.get(es_index, id)
    #     return model(**doc['_source'])
    #
    # async def _put_by_id_to_cache(self, model: BaseModel, expire: int):
    #     await self.redis.set(model.id, model.json(), expire=expire)
    #
    # async def _get_by_search(self, search_string: str, search_field: str,
    #                          expire: int, es_index: str, model: BaseModel
    #                          ) -> Optional[list[BaseModel]]:
    #     obj_list = await self._get_from_cache(
    #         key=f'{es_index}:{search_string}', model=model)
    #     if not obj_list:
    #         obj_list = await self._get_by_search_from_elastic(
    #             search_string, search_field, es_index, model)
    #         if not obj_list:
    #             return None
    #         await self._put_to_cache(key=f'{es_index}:{search_string}',
    #                                  model_list=obj_list, expire=expire)
    #     return obj_list
    #
    # async def _get_by_search_from_elastic(
    #         self, search_string: str, search_field: str,
    #         es_index: str, model: BaseModel) -> list[BaseModel]:
    #     doc = await self.elastic.search(
    #         index=es_index,
    #         body={"query": {
    #             "match": {
    #                 search_field: {
    #                     "query": search_string,
    #                     "fuzziness": "auto"
    #                 }
    #             }
    #         }})
    #     return [model(**d['_source']) for d in doc['hits']['hits']]
    #
    # async def _get_list(
    #         self, page_number: int, page_size: int,
    #         expire: int, es_index: str, model: BaseModel
    # ) -> Optional[list[BaseModel]]:
    #     obj_list = await self._get_from_cache(
    #         key=f'{es_index}:{page_number}:{page_size}', model=model
    #     )
    #     if not obj_list:
    #         obj_list = await self._get_list_from_elastic(page_number,
    #                                                      page_size,
    #                                                      es_index,
    #                                                      model)
    #         if not obj_list:
    #             return None
    #         await self._put_to_cache(
    #             key=f'{es_index}:{page_number}:{page_size}',
    #             model_list=obj_list, expire=expire)
    #     return obj_list
    #
    # async def _get_list_from_elastic(
    #         self, page_number: int, page_size: int, es_index: str,
    #         model: BaseModel, query: dict = None) -> list[BaseModel]:
    #     body = {"from": page_number * page_size, "size": page_size}
    #     if query:
    #         body = body | query
    #     docs = await self.elastic.search(
    #         index=es_index,
    #         body=body
    #     )
    #     return [model(**d['_source']) for d in docs['hits']['hits']]
    #
    # async def _put_to_cache(self, model_list: list[BaseModel],
    #                         expire: int, key: str):
    #     list_json = json.dumps(model_list, default=pydantic_encoder)
    #     await self.redis.set(key, list_json, expire=expire)
    #
    # async def _get_from_cache(
    #         self, key: str, model: BaseModel) -> Optional[list[BaseModel]]:
    #     data = await self.redis.get(key)
    #     if not data:
    #         return None
    #     return parse_raw_as(list[model], data)


