from abc import ABC, abstractmethod


class Cacheable(ABC):
    @abstractmethod
    def get(self, key: str):
        pass

    @abstractmethod
    def set(self, key: str, value: str, expire: int):
        pass


class EsSearch(ABC):

    @abstractmethod
    def body_search(self, key: str, query: str):
        pass

    @abstractmethod
    def body_all(self):
        pass

    @abstractmethod
    def get_search(self,
                   es_index: str,
                   func_name: str,
                   field: list,
                   q: str = None,
                   query: dict = None):
        pass
