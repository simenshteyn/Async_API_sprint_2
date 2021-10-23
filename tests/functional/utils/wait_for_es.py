import time

from elasticsearch import Elasticsearch

es = Elasticsearch(['http://localhost:9200/'], verify_certs=True)

while not es.ping():
    print('ES not connected, retry in 5 seconds...')
    time.sleep(5)
else:
    print('ES connected.')
