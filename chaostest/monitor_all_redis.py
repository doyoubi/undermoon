import datetime
import threading

import redis

import config

class Monitor():
    def __init__(self, connection_pool):
        self.connection_pool = connection_pool
        self.connection = None

    def __del__(self):
        try:
            self.reset()
        except:
            pass

    def reset(self):
        if self.connection:
            self.connection_pool.release(self.connection)
            self.connection = None

    def monitor(self):
        if self.connection is None:
            self.connection = self.connection_pool.get_connection(
                'monitor', None)
        self.connection.send_command("monitor")
        return self.listen()

    def parse_response(self):
        return self.connection.read_response()

    def listen(self):
        while True:
            yield self.parse_response()


def run_monitor(address):
    host, port = address.split(':')
    pool = redis.ConnectionPool(host=host, port=port)
    monitor = Monitor(pool)
    commands = monitor.monitor()
    for c in commands:
        print(address, datetime.datetime.now(), c)

# Need to put this in your /etc/hosts
# 127.0.0.1 redis6000
# 127.0.0.1 redis6001
# ...

if __name__ == '__main__':
    redis_addresses = config.DOCKER_COMPOSE_CONFIG['redis_addresses']
    for addr in redis_addresses:
        # Python variable is a name. Need to use the whole array instead of a shared variable.
        threading.Thread(target=lambda: run_monitor(addr)).start()
