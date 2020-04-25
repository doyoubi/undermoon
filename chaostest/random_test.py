import sys
import time
import signal
import random
from loguru import logger

from redis import StrictRedis

import config
from utils import OvermoonClient, ServerProxy, OVERMOON_ENDPOINT, RedisClusterClient


exit_on_error = True


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
        self.kvs = set()
        self.deleted_kvs = set()

    def get_proxies(self):
        cluster = self.overmoon_client.get_cluster(self.cluster_name)
        if cluster is None:
            return None

        proxies = []
        nodes = cluster['nodes']
        for node in nodes:
            if node['repl']['role'] == 'replica':
                continue
            proxy_address = node['proxy_address']
            host, port = proxy_address.split(':')
            proxies.append({'host': host, 'port': port, 'epoch': cluster['epoch']})
        return proxies

    def cluster_ready(self, proxies):
        # Overmoon will cache the data so once it's ready,
        # you can't say it will be ready in the future.
        # Maybe the latest metadata are still not synchronized to the proxies.
        for proxy in proxies:
            client = StrictRedis(host=proxy['host'], port=proxy['port'])
            try:
                r = client.execute_command('cluster', 'nodes')
            except Exception as e:
                raise Exception('{}:{}: {}'.format(proxy['host'], proxy['port'], e))
            lines = list(r.decode('utf-8').split('\n'))
            if len(lines) <= 1:
                return False
            TRIMMED_LEN = 20
            # This should not be the info from the last cluster.
            if self.cluster_name[0:TRIMMED_LEN] not in lines[0]:
                return False
            found_epoch = int(lines[0].split(' ')[6])
            if found_epoch != int(proxy['epoch']):
                return False
        return True

    def gen_client(self, proxies):
        conn_timeout = 2
        return RedisClusterClient(proxies, conn_timeout)

    def test_key_value(self):
        proxies = self.get_proxies()
        if proxies is None:
            return

        try:
            if not self.cluster_ready(proxies):
                logger.info('cluster {} not ready', self.cluster_name)
                return

            n = random.randint(0, 10)
            if n < 4:
                self.test_set(proxies, self.cluster_name)
            elif n < 8:
                self.test_get(proxies)
            else:
                self.test_del(proxies)
        except Exception as e:
            logger.error('REDIS_TEST_FAILED: {} {}', self.cluster_name, e)
            logger.error('REDIS_TEST_FAILED: {} ', self.overmoon_client.get_cluster(self.cluster_name))
            raise

    def test_set(self, proxies, cluster_name):
        if len(self.kvs) >= self.MAX_KVS:
            return

        rc = self.gen_client(proxies)
        t = int(time.time())
        for i in range(10):
            # Need cluster name to avoid key being transferred to another cluster
            # so that the key get recovered if it was set in multiple clusters.
            k = 'test:{}:{}:{}'.format(cluster_name, t, i)
            try:
                res, proxy = rc.set(k, k)
            except Exception as e:
                logger.error('REDIS_TEST: failed to set {}: {}', k, e)
                raise
            if not res:
                logger.info('REDIS_TEST: invalid response: {} proxy: {}', res, proxy)
                continue
            self.kvs.add(k)
            self.deleted_kvs.discard(k)

    def test_get(self, proxies):
        rc = self.gen_client(proxies)
        for k in self.kvs:
            try:
                v, proxy = rc.get(k)
            except Exception as e:
                logger.error('REDIS_TEST: failed to get {}: {}', k, e)
                raise
            if k != v:
                logger.error('INCONSISTENT: key: {}, expected {}, got {}, proxy {}', k, k, v, proxy)
                raise Exception("INCONSISTENT DATA")

        for k in self.deleted_kvs:
            try:
                v, proxy = rc.get(k)
            except Exception as e:
                logger.error('REDIS_TEST: failed to get {}: {}', k, e)
                raise
            if v is not None:
                logger.error('INCONSISTENT: key: {}, expected {}, got {}, proxy {}', k, None, v, proxy)
                raise Exception("INCONSISTENT DATA")

    def test_del(self, proxies):
        rc = self.gen_client(proxies)

        keys = list(self.kvs)
        for k in keys:
            if random.randint(0, 6) < 3:
                continue
            try:
                v, proxy = rc.delete(k)
            except Exception as e:
                logger.error('REDIS_TEST: failed to get {}: {}', k, e)
                raise
            self.kvs.discard(k)
            self.deleted_kvs.add(k)


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
        names.append('randomdb{}'.format(random.randint(1, 100)))
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
        logger.info('test data of: {}', cluster_name)
        tester = self.kvs_tester[cluster_name]
        tester.test_key_value()

    def loop_test(self):
        while not self.stopped:
            self.overmoon_client.sync_all_server_proxy(self.server_proxy_list)
            if not self.overmoon_client.get_free_proxies():
                logger.warning("no free proxy found")

            names = self.overmoon_client.get_cluster_names()
            if names:
                logger.info('clusters: {}', names)

            if not names or random.randint(0, 10) < 2:
                node_number = random.randint(0, 40)
                self.overmoon_client.create_cluster(self.gen_cluster_name(), node_number)

            if names and random.randint(0, 10) < 8:
                cluster_name = random.choice(names)
                cluster = self.overmoon_client.get_cluster(cluster_name)
                existing_node_num = len(cluster['nodes'])
                max_chunk_num = (config.REDIS_NUM - existing_node_num) / 4
                node_number = random.randint(0, max_chunk_num + 1) * 4
                self.overmoon_client.add_nodes(cluster_name, node_number)
                if random.randint(0, 10) < 7:
                    if random.randint(0, 10) % 2 == 0:
                        self.overmoon_client.scale_cluster(cluster_name)
                    else:
                        new_node_num = random.randint(0, existing_node_num / 4 + 1) * 4
                        self.overmoon_client.scale_down_cluster(cluster_name, new_node_num)

            if names and random.randint(0, 10) < 6:
                if self.overmoon_client.remove_unused_nodes(random.choice(names)):
                    logger.info("Removed nodes. Need to wait or the commands will be sent to removed nodes")
                    # TODO: get proxies not directly by the api to avoid this.
                    time.sleep(10)
                    continue

            self.test_data()

            if self.stopped:
                break

            if names and random.randint(0, 600) < 1:
                cluster_name = random.choice(names)
                self.overmoon_client.delete_cluster(cluster_name)
                self.kvs_tester.pop(cluster_name, None)

            time.sleep(0.1)

    def keep_testing(self):
        while not self.stopped:
            try:
                self.loop_test()
            except Exception as e:
                logger.error('TEST_FAILED: {}', e)
                if exit_on_error:
                    raise
                continue


if __name__ == '__main__':
    exit_on_error = len(sys.argv) >= 2 and sys.argv[1] == 'exit-on-error'
    if exit_on_error:
        print("Will exit on error")
    else:
        print("Will not exit on error")
    RandomTester(OvermoonClient(OVERMOON_ENDPOINT)).keep_testing()
