import time

from redis import Redis, ConnectionError

from functional.settings import config

r = Redis(host=config.redis_host, port=config.redis_port)

is_connected = False

while not is_connected:
    try:
        is_connected = r.ping()
        print('Redis connected.')
    except ConnectionError:
        print('Redis not connected, retry in 5 seconds...')
        time.sleep(5)
