import time

from elasticsearch import Elasticsearch

from functional.settings import config

es = Elasticsearch([f'{config.es_host}:{config.es_port}'], verify_certs=True)

while not es.ping():
    print('ES not connected, retry in 5 seconds...')
    time.sleep(5)
else:
    print('ES connected.')
