import time

from redis import Redis, ConnectionError

r = Redis(host="localhost", port="6379")

is_connected = False

while not is_connected:
    try:
        is_connected = r.ping()
        print('Redis connected.')
    except ConnectionError:
        print('Redis not connected, retry in 5 seconds...')
        time.sleep(5)
