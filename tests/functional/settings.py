from pydantic import BaseSettings


class TestSettings(BaseSettings):
    es_host: str
    es_port: int
    redis_host: str
    redis_port: int
    service_host: str
    service_port: int
    service_protocol: str
    service_api_version: int


config = TestSettings.parse_file('config.json')
