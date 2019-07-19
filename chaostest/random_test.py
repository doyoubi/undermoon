import time
import signal
import random

from rediscluster import StrictRedisCluster

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


class KeyValueTester:
    MAX_KVS = 1000

    def __init__(self, cluster_name, overmoon_client):
        self.cluster_name = cluster_name
        self.overmoon_client = overmoon_client
        self.kvs = []

    def get_proxies(self):
        cluster = self.overmoon_client.get_cluster(self.cluster_name)
        if cluster is None:
            return None

        proxies = []
        nodes = cluster['nodes']
        for node in nodes:
            proxy_address = node['proxy_address']
            host, port = proxy_address.split(':')
            proxies.append({'host': host, 'port': port})
        return proxies

    def test_key_value(self):
        proxies = self.get_proxies()
        if proxies is None:
            return

        if random.randint(0, 10) < 5:
            self.test_set(proxies)
        else:
            self.test_get(proxies)

    def test_set(self, proxies):
        if len(self.kvs) >= self.MAX_KVS:
            return

        rc = StrictRedisCluster(startup_nodes=proxies, decode_responses=True, skip_full_coverage_check=True)
        t = int(time.time())
        for i in range(10):
            k = 'test:{}:{}'.format(t, i)
            try:
                res = rc.set(k, k)
            except Exception as e:
                print('failed to set {}: {}', k, e)
                raise
            if not res:
                print('invalid response:', res)
                continue
            self.kvs.append(k)

    def test_get(self, proxies):
        rc = StrictRedisCluster(startup_nodes=proxies, decode_responses=True, skip_full_coverage_check=True)
        for k in self.kvs:
            try:
                v = rc.get(k)
            except Exception as e:
                print('failed to get {}: {}', k, e)
                raise
            if k != v:
                print('INCONSISTENT: key: {}, expected {}, got {}'.format(k, k, v))


class RandomTester:
    def __init__(self, overmoon_client):
        self.overmoon_client = overmoon_client
        self.server_proxy_list = gen_server_proxy_list()
        self.stopped = False
        self.kvs_tester = {}
        self.init_signal_handler()

    def init_signal_handler(self):
        signal.signal(signal.SIGINT, self.handle_signal)

    def handle_signal(self, sig, frame):
        self.stop()

    def stop(self):
        self.stopped = True

    def gen_cluster_name(self):
        names = ['mydb', 'somedb', 'otherdb', 'dybdb', '99db']
        names.append('ramdomdb{}'.format(random.randint(1, 100)))
        return random.choice(names)

    def test_data(self):
        names = self.overmoon_client.get_cluster_names()
        if not names:
            return

        new_kvs_tester = {}
        for cluster_name in names:
            if cluster_name in self.kvs_tester:
                new_kvs_tester[cluster_name] = self.kvs_tester[cluster_name]
            else:
                new_kvs_tester[cluster_name] = KeyValueTester(cluster_name, self.overmoon_client)
        self.kvs_tester = new_kvs_tester
        cluster_name = random.choice(list(self.kvs_tester.keys()))
        print('test data of:', cluster_name)
        tester = self.kvs_tester[cluster_name]
        tester.test_key_value()

    def loop_test(self):
        while not self.stopped:
            self.overmoon_client.sync_all_server_proxy(self.server_proxy_list)

            names = self.overmoon_client.get_cluster_names()
            if names:
                print('clusters', names)

            if not names or random.randint(0, 10) < 2:
                node_number = random.randint(0, 40)
                self.overmoon_client.create_cluster(self.gen_cluster_name(), node_number)

            if names and random.randint(0, 10) < 4:
                cluster_name = random.choice(names)
                self.overmoon_client.add_nodes(cluster_name)
                if random.randint(0, 10) < 7:
                    self.overmoon_client.scale_cluster(cluster_name)

            if names and random.randint(0, 10) < 6:
                self.overmoon_client.remove_unused_nodes(random.choice(names))

            self.test_data()

            if self.stopped:
                break

            if names and random.randint(0, 10) < 1:
                self.overmoon_client.delete_cluster(random.choice(names))

            time.sleep(0.1)


RandomTester(OvermoonClient(OVERMOON_ENDPOINT)).loop_test()
