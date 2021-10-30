import psycopg2
import logging


from datetime import datetime
from contextlib import closing

from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from config import dsl, es_conf
from postgresloader import LoadMovies, LoadGenre, LoadPerson
from utils import backoff
from es import EsSaver
from state import State, JsonFileStorage

logger = logging.getLogger('LoaderStart')


def load_from_postgres(pg_conn: _connection, name_index: str) -> list:
    """Основной метод загрузки данных из Postgres"""
    if name_index == "movies":
        postgres_loader = LoadMovies(pg_conn)
        data = postgres_loader.loader_movies()
    elif name_index == "genre":
        postgres_loader = LoadGenre(pg_conn)
        data = postgres_loader.loader_genre()
    elif name_index == "person":
        postgres_loader = LoadPerson(pg_conn)
        data = postgres_loader.loader_person()
    else:
        data = None
    return data


if __name__ == '__main__':
    @backoff()
    def query_postgres_film(name_index) -> list:
        with closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
            logger.info(f'{datetime.now()}\n\nPostgreSQL connection is open. Start load {name_index} data')
            load_pq = load_from_postgres(pg_conn, name_index)
        return load_pq


    def save_elastic(name_index: str) -> None:
        logger.info(f'{datetime.now()}\n\nElasticSearch connection is open. Start load {name_index} data')
        EsSaver(es_conf).load(query_postgres_film(name_index), name_index=name_index)

    save_elastic(name_index='movies')
    save_elastic(name_index='genre')
    save_elastic(name_index='person')

    State(JsonFileStorage('PostgresDataState.txt')).set_state(str('my_key'), value=str(datetime.now()))
