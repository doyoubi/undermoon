import time
import random

import config
from utils import OvermoonClient, ServerProxy, OVERMOON_ENDPOINT


def gen_server_proxy_list():
    redis_ports = config.DOCKER_COMPOSE_CONFIG['redis_ports']
    server_proxy_ports = config.DOCKER_COMPOSE_CONFIG['server_proxy_ports']

    redis_addresses = ['redis{}:{}'.format(p, p) for p in redis_ports]
    server_proxy_addresses = ['server_proxy{}:{}'.format(p, p) for p in server_proxy_ports]

    proxies = []
    for i in range(len(server_proxy_ports)):
        nodes = [redis_addresses[2*i], redis_addresses[2*i+1]]
        proxy = ServerProxy(server_proxy_addresses[i], nodes)
        proxies.append(proxy)

    return proxies


class RandomTester:
    def __init__(self, overmoon_client):
        self.overmoon_client = overmoon_client
        self.server_proxy_list = gen_server_proxy_list()

    def gen_cluster_name(self):
        names = ['mydb', 'somedb', 'otherdb', 'dybdb', '99db']
        names.append('ramdomdb{}'.format(random.randint(1, 100)))
        return random.choice(names)

    def loop_test(self):
        while True:
            self.overmoon_client.sync_all_server_proxy(self.server_proxy_list)

            names = self.overmoon_client.get_cluster_names()
            print('clusters', names)

            if not names or random.randint(0, 10) < 2:
                node_number = random.randint(0, 40)
                self.overmoon_client.create_cluster(self.gen_cluster_name(), node_number)

            if names and random.randint(0, 10) < 2:
                self.overmoon_client.delete_cluster(random.choice(names))

            time.sleep(0.1)


RandomTester(OvermoonClient(OVERMOON_ENDPOINT)).loop_test()
