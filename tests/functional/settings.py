from pydantic import BaseSettings, Field


class TestSettings(BaseSettings):
    es_host: str
    es_port: int
    redis_host: str
    redis_port: int


config = TestSettings.parse_file('config.json')
