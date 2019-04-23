import sys
import time
from copy import deepcopy
import logging

import redis
from flask import Flask, jsonify, request


logger = logging.getLogger('simple_broker')


def init_logger():
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)


init_logger()
app = Flask(__name__)


MASTER = 'master'
REPLICA = 'replica'
DB_NAME = 'mydb'


class BrokerError(Exception):
    pass


class ReplPeer:
    def __init__(self, node_address, proxy_address):
        self.node_address = node_address
        self.proxy_address = proxy_address

    def to_dict(self):
        return {
            'node_address': self.node_address,
            'proxy_address': self.proxy_address,
        }


class SlotRange:
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def to_dict(self):
        return {'start': self.start, 'end': self.end, 'tag': ''}


class ReplMeta:
    def __init__(self, role, peers):
        self.role = role
        self.peers = peers

    def to_dict(self):
        return {
            'role': self.role,
            'peers': [p.to_dict() for p in self.peers]
        }


class Node:
    def __init__(self, address, proxy_address, cluster_name, slots, repl):
        self.address = address
        self.proxy_address = proxy_address
        self.cluster_name = cluster_name
        self.slots = slots
        self.repl = repl

    def to_dict(self):
        return {
            'address': self.address,
            'proxy_address': self.proxy_address,
            'cluster_name': self.cluster_name,
            'slots': [self.slots.to_dict()] if self.slots else [],
            'repl': self.repl.to_dict(),
        }


def gen_meta_config(cluster_name):
    redis1 = 'redis1:7001'
    redis2 = 'redis2:7002'
    redis3 = 'redis3:7003'
    redis4 = 'redis4:7004'
    redis5 = 'redis5:7005'
    redis6 = 'redis6:7006'
    proxy1 = 'server_proxy1:6001'
    proxy2 = 'server_proxy2:6002'
    proxy3 = 'server_proxy3:6003'

    INIT_SLOTS_CONFIG = {
        redis1: [0, 5461],
        redis2: [5462, 10922],
        redis3: [10923, 16383],
    }
    TOPO_CONFIG = {
        proxy1: {
            'master': redis1,
            'replica': redis4,
        },
        proxy2: {
            'master': redis2,
            'replica': redis5,
        },
        proxy3: {
            'master': redis3,
            'replica': redis6,
        },
    }

    meta_config = {}
    for i, (proxy_address, nodes) in enumerate(TOPO_CONFIG.items()):
        i += 1
        master_address = nodes['master']
        replica_address = nodes['replica']

        replica_proxy = 'server_proxy{i}:600{i}'.format(i=i % 3 + 1)
        replica_node = TOPO_CONFIG[replica_proxy]['replica']
        master_repl = ReplMeta(MASTER, [ReplPeer(replica_node, replica_proxy)])

        master_proxy = 'server_proxy{i}:600{i}'.format(i=(i + 1) % 3 + 1)
        master_node = TOPO_CONFIG[master_proxy]['master']
        replica_repl = ReplMeta(REPLICA, [ReplPeer(master_node, master_proxy)])

        master_slots = SlotRange(INIT_SLOTS_CONFIG[master_address][0],
                                 INIT_SLOTS_CONFIG[master_address][1])
        master = Node(master_address, proxy_address, cluster_name,
                      master_slots, master_repl)
        replica = Node(replica_address, proxy_address, cluster_name, None, replica_repl)

        meta_config[proxy_address] = {
            'master': master,
            'replica': replica,
        }

    return meta_config


class Failure:
    def __init__(self, proxy_address, reporter_id):
        self.proxy_address = proxy_address
        self.reporter_id = reporter_id
        self.report_time = time.time()


def check_alive(address):
    print(address)
    host, port = address.split(':')
    try:
        redis.StrictRedis(host, port, socket_timeout=0.1).ping()
        return True
    except redis.RedisError as e:
        logger.error('failed to connect to redis: %s', e)
        return False


class MetaStore:
    def __init__(self, cluster_name):
        self.epoch = 1
        self.cluster_name = cluster_name
        self.proxies = gen_meta_config(cluster_name)

        self.failed_proxies = {}
        self.replaced_master = set()

    def get_failed_proxies(self):
        new_failed_proxies = {}
        for address, failure in self.failed_proxies.items():
            # For simplicity, just check it to remove stale failure.
            if not check_alive(address):
                new_failed_proxies[address] = failure
        self.failed_proxies = new_failed_proxies

        if not new_failed_proxies and self.replaced_master:
            self.replaced_master = set()
            self.epoch += 1
        return new_failed_proxies

    def get_replaced_node(self, failed_proxy):
        failed_master = self.proxies[failed_proxy][MASTER]
        replica = failed_master.repl.peers[0]
        replica_proxy = replica.proxy_address

        replica = deepcopy(self.proxies[replica_proxy]['replica'])
        replica.role = MASTER
        replica.slots = failed_master.slots
        replica.repl = ReplMeta(MASTER, [])
        return replica

    def get_proxies(self):
        failed = list(self.get_failed_proxies().keys())
        if not failed:
            return self.proxies

        if len(failed) > 1:
            raise BrokerError('cluster is down')

        new_master = self.get_replaced_node(failed[0])
        proxies = {a: deepcopy(p) for a, p in self.proxies.items() if a not in failed}
        proxies[new_master.proxy_address][REPLICA] = new_master
        return proxies

    def get_nodes(self):
        proxies = self.get_proxies()
        return sum(
            [[p['master'], p['replica']] for p in proxies.values()],
            [])

    def get_cluster_names(self):
        return {
            'names': [self.cluster_name]
        }

    def get_cluster(self, cluster_name):
        if self.cluster_name != cluster_name:
            return {'cluster': None}

        return {
            'cluster': {
                'name': cluster_name,
                'epoch': self.epoch,
                'nodes': [n.to_dict() for n in self.get_nodes()]
            }
        }

    def get_proxy_addresses(self):
        return {
            'addresses': list(self.get_proxies().keys()),
        }

    def get_proxy(self, address):
        proxy = self.get_proxies().get(address)
        if proxy is None:
            return {'host': None}

        return {
            "host": {
                "address": address,
                "epoch": self.epoch,
                "nodes": [proxy['master'].to_dict(), proxy['replica'].to_dict()]
            }
        }

    def report_failure(self, proxy_address, reporter_id):
        self.failed_proxies[proxy_address] = Failure(proxy_address, reporter_id)

    def get_failures(self):
        return {
            "addresses": list(self.get_failed_proxies().keys()),
        }

    def replace_node(self, failed_node):
        '''
        failed_node:
        {
            "cluster_epoch": 1,
            "node": {
                "address": "127.0.0.1:7001",
                "proxy_address": "127.0.0.1:6001",
                "cluster_name": "cluster_name1",
                "repl": {
                    "role": "master",
                    "peers": [{
                        "node_address": "127.0.0.1:7002",
                        "proxy_address": "127.0.0.1:7003",
                    }...]
                },
                "slots": [{
                    "start": 0,
                    "end": 5000,
                    "tag": ""
                }, ...]
            }
        }
        '''
        if failed_node['cluster_epoch'] < self.epoch:
            err_msg = 'lower epoch {} < {}'.format(failed_node['cluster_epoch'], self.epoch)
            logger.warning(err_msg)
            raise BrokerError(err_msg)

        node_address = failed_node['node']['address']
        proxy_address = failed_node['node']['proxy_address']

        if self.proxies[proxy_address][MASTER].address != node_address:
            raise BrokerError('Can not create new replica')

        self.replaced_master.add(node_address)
        self.epoch += 1
        return self.get_replaced_node(proxy_address).to_dict()


meta_store = MetaStore(DB_NAME)


@app.route('/api/clusters/names')
def get_cluster_names():
    return jsonify(meta_store.get_cluster_names())


@app.route('/api/clusters/name/<cluster_name>')
def get_cluster(cluster_name):
    return jsonify(meta_store.get_cluster(cluster_name))


@app.route('/api/hosts/addresses')
def get_proxies():
    return jsonify(meta_store.get_proxy_addresses())


@app.route('/api/hosts/address/<server_proxy_address>')
def get_proxy(server_proxy_address):
    proxy = meta_store.get_proxy(server_proxy_address)
    import json
    logger.info(json.dumps(proxy))
    return jsonify(proxy)


@app.route('/api/failures/<server_proxy_address>/<reporter_id>', methods=['POST'])
def report_failure(server_proxy_address, reporter_id):
    return jsonify(meta_store.report_failure(server_proxy_address, reporter_id))


@app.route('/api/failures')
def get_failures():
    return jsonify(meta_store.get_failures())


@app.route('/api/clusters/nodes', methods=['PUT'])
def replace_node():
    failed_node = request.get_json()
    return jsonify(meta_store.replace_node(failed_node))


if __name__ == '__main__':
    port = sys.argv[1]
    app.run(host='0.0.0.0', port=int(port))
