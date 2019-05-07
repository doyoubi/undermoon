import sys
import time
import logging
from copy import deepcopy

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


class MigrationMeta:
    def __init__(self, epoch, src_proxy_address, src_node_address,
            dst_proxy_address, dst_node_address):
        self.epoch = epoch
        self.src_proxy_address = src_proxy_address
        self.src_node_address = src_node_address
        self.dst_proxy_address = dst_proxy_address
        self.dst_node_address = dst_node_address

    def to_dict(self):
        return {
            'epoch': self.epoch,
            'src_proxy_address': self.src_proxy_address,
            'src_node_address': self.src_node_address,
            'dst_proxy_address': self.dst_proxy_address,
            'dst_node_address': self.dst_node_address,
        }


class SlotRangeTag:
    MIGRATING = 'Migrating'
    IMPORTING = 'Importing'
    NoneTag = 'None'
    def __init__(self, migration_tag, meta):
        self.migration_tag = migration_tag
        self.meta = meta


class SlotRange:
    def __init__(self, start, end):
        self.start = start
        self.end = end
        self.tag = SlotRangeTag(SlotRangeTag.NoneTag, None)

    def to_dict(self):
        if self.tag.migration_tag == SlotRangeTag.NoneTag:
            tag = 'None'
        else:
            assert self.tag.migration_tag in [SlotRangeTag.IMPORTING, SlotRangeTag.MIGRATING]
            tag = {self.tag.migration_tag: self.tag.meta.to_dict()}
        return {
            'start': self.start,
            'end': self.end,
            'tag': tag,
        }


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
        assert isinstance(slots, list)
        self.slots = slots
        self.repl = repl

    def to_dict(self):
        return {
            'address': self.address,
            'proxy_address': self.proxy_address,
            'cluster_name': self.cluster_name,
            'slots': [slot_range.to_dict() for slot_range in self.slots],
            'repl': self.repl.to_dict(),
        }


def gen_meta_config(cluster_name):
    redis1 = 'redis1:6379'
    redis2 = 'redis2:6379'
    redis3 = 'redis3:6379'
    redis4 = 'redis4:6379'
    redis5 = 'redis5:6379'
    redis6 = 'redis6:6379'
    proxy1 = 'server_proxy1:6001'
    proxy2 = 'server_proxy2:6002'
    proxy3 = 'server_proxy3:6003'

    redis7 = 'redis7:6379'
    redis8 = 'redis8:6379'
    redis9 = 'redis9:6379'
    proxy4 = 'server_proxy4:6004'
    proxy5 = 'server_proxy5:6005'
    proxy6 = 'server_proxy6:6006'

    INIT_SLOTS_CONFIG = {
        redis1: [0, 5461],
        redis2: [5462, 10922],
        redis3: [10923, 16383],
    }
    TOPO_CONFIG = {
        proxy1: {
            MASTER: redis1,
            REPLICA: redis4,
        },
        proxy2: {
            MASTER: redis2,
            REPLICA: redis5,
        },
        proxy3: {
            MASTER: redis3,
            REPLICA: redis6,
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
                      [master_slots], master_repl)
        replica = Node(replica_address, proxy_address, cluster_name, [], replica_repl)

        meta_config[proxy_address] = {
            'master': master,
            'replica': replica,
        }

    extended_meta_config = {}
    extended_proxies = {
        proxy4: {
            MASTER: redis7,
        },
        proxy5: {
            MASTER: redis8,
        },
        proxy6: {
            MASTER: redis9,
        },
    }
    for i, (proxy_address, nodes) in enumerate(extended_proxies.items()):
        i += 3
        master_address = nodes['master']
        master_repl = ReplMeta(MASTER, [])
        master = Node(master_address, proxy_address, cluster_name,
                      [], master_repl)
        extended_meta_config[proxy_address] = {
            MASTER: master,
        }

    migrating_map = {
        proxy1: proxy4,
        proxy2: proxy5,
        proxy3: proxy6,
    }

    return meta_config, extended_meta_config, migrating_map


class Failure:
    def __init__(self, proxy_address, reporter_id):
        self.proxy_address = proxy_address
        self.reporter_id = reporter_id
        self.report_time = time.time()


def check_alive(address):
    host, port = address.split(':')
    try:
        redis.StrictRedis(host, port, socket_timeout=0.1).ping()
        return True
    except redis.RedisError as e:
        logger.error('failed to connect to redis: %s', e)
        return False


def split_slots(slots):
    assert len(slots) == 1
    slot_range = slots[0]
    start = slot_range.start
    end = slot_range.end
    middle = int((start + end) / 2)
    assert start <= middle < end
    return SlotRange(start, middle), SlotRange(middle+1, end)


class MetaStore:
    def __init__(self, cluster_name):
        self.epoch = 1
        self.cluster_name = cluster_name
        self.proxies, self.extended_proxies, self.migrating_map = gen_meta_config(cluster_name)

        self.failed_proxies = {}
        self.replaced_master = set()

        self.migration_epoch = None
        self.importing_proxies = set()
        self.imported_proxies = set()

    def gen_src_slots(self, proxy, epoch):
        assert proxy in self.migrating_map
        src_master = self.proxies[proxy][MASTER]
        slots = src_master.slots

        src = proxy
        dst = self.migrating_map[src]
        dst_master = self.extended_proxies[dst][MASTER]
        unchanged_slots, new_slots = split_slots(slots)

        if dst not in self.importing_proxies and dst not in self.imported_proxies:
            return self.proxies[proxy][MASTER].slots

        if dst in self.importing_proxies:
            meta = MigrationMeta(
                epoch,
                src,
                src_master.address,
                dst,
                dst_master.address,
            )
            new_slots.tag = SlotRangeTag(SlotRangeTag.MIGRATING, meta)
            return [unchanged_slots, new_slots]

        assert dst in self.imported_proxies
        return [unchanged_slots]

    def gen_dst_slots(self, proxy, epoch):
        importing_map = {v: k for k, v in self.migrating_map.items()}
        assert proxy in importing_map

        dst = proxy
        src = importing_map[dst]
        src_master = self.proxies[src][MASTER]
        dst_master = self.extended_proxies[dst][MASTER]
        slots = src_master.slots
        unchanged_slots, new_slots = split_slots(slots)

        if dst not in self.importing_proxies and dst not in self.imported_proxies:
            return []

        if dst in self.importing_proxies:
            meta = MigrationMeta(
                epoch,
                src,
                src_master.address,
                dst,
                dst_master.address,
            )
            new_slots.tag = SlotRangeTag(SlotRangeTag.IMPORTING, meta)

        return [new_slots]

    def gen_slots(self, proxy, epoch):
        if proxy in self.migrating_map:
            return self.gen_src_slots(proxy, epoch)
        return self.gen_dst_slots(proxy, epoch)

    def get_origin_proxies(self):
        proxies = deepcopy(self.proxies)
        for proxy, nodes in self.extended_proxies.items():
            if proxy in self.importing_proxies or proxy in self.imported_proxies:
                proxies[proxy] = deepcopy(nodes)
        for proxy, nodes in proxies.items():
            master = nodes[MASTER]
            master.slots = self.gen_slots(proxy, self.migration_epoch)
        return proxies

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
        logger.info('failed {} replaced_master {}'.format(failed, self.replaced_master))
        if not failed or not self.replaced_master:
            return deepcopy(self.get_origin_proxies())

        if len(failed) > 1:
            raise BrokerError('cluster is down')

        new_master = self.get_replaced_node(failed[0])
        proxies = {a: deepcopy(p) for a, p in self.get_origin_proxies().items() if a not in failed}
        proxies[new_master.proxy_address][REPLICA] = new_master

        return proxies

    def get_nodes(self):
        proxies = self.get_proxies()
        return sum(
            [list(p.values()) for p in proxies.values()],
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

        logger.info('get_proxy epoch %d %s', self.epoch, self.replaced_master)
        nodes = [p.to_dict() for p in proxy.values()]
        return {
            'host': {
                'address': address,
                'epoch': self.epoch,
                'nodes': nodes,
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
                    "tag": "None"
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

        if self.get_origin_proxies()[proxy_address][MASTER].address != node_address:
            raise BrokerError('Can not create new replica')

        self.replaced_master.add(node_address)
        self.epoch += 1
        logger.info('successfully replace node %s', node_address)
        return self.get_replaced_node(proxy_address).to_dict()

    def commit_migration(self, migration_task_meta):
        '''
        {
            "db_name": "mydb",
            "slot_range": {
                "start": 0,
                "end": 5000,
                "tag": {
                    "Migrating": {
                        "epoch": 233,
                        "src_proxy_address": "127.0.0.1:7000",
                        "src_node_address": "127.0.0.1:7001",
                        "dst_proxy_address": "127.0.0.2:7000",
                        "dst_node_address": "127.0.0.2:7001"
                    }
                }
            }
        }
        '''
        meta = migration_task_meta['slot_range']['tag']['Migrating']
        epoch = meta['epoch']
        # src_proxy_address = meta['src_proxy_address']
        # src_node_address = meta['src_node_address']
        dst_proxy_address = meta['dst_proxy_address']
        # dst_node_address = meta['dst_node_address']

        if epoch != self.migration_epoch:
            raise Exception('invalid epoch {} != {}'.format(epoch, self.migrating_map))

        if dst_proxy_address not in self.importing_proxies:
            raise Exception('{} is not migrating'.format(dst_proxy_address))

        self.migration_epoch = None
        self.epoch += 1
        self.imported_proxies.add(dst_proxy_address)
        self.importing_proxies.remove(dst_proxy_address)
        return {'epoch': self.epoch}

    def trigger_scaling(self):
        self.epoch += 1
        self.migration_epoch = self.epoch
        for proxy, nodes in self.extended_proxies.items():
            self.importing_proxies.add(proxy)
            return {'start_epoch': self.epoch}


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
    return jsonify(proxy)


@app.route('/api/failures/<server_proxy_address>/<reporter_id>', methods=['POST'])
def report_failure(server_proxy_address, reporter_id):
    return jsonify(meta_store.report_failure(server_proxy_address, reporter_id))


@app.route('/api/failures')
def get_failures():
    return jsonify(meta_store.get_failures())


@app.route('/api/clusters/nodes', methods=['PUT'])
def replace_node():
    logger.info('replace_node %s', request.get_json())
    failed_node = request.get_json()
    return jsonify(meta_store.replace_node(failed_node))


@app.route('/api/clusters/migration', methods=['PUT'])
def commit_migration():
    logger.info('migration %s', request.get_json())
    migration_task_meta = request.get_json()
    return jsonify(meta_store.commit_migration(migration_task_meta))


@app.route('/api/test/migration', methods=['POST'])
def trigger_migration():
    logger.info('start migration')
    return jsonify(meta_store.trigger_scaling())


if __name__ == '__main__':
    port = sys.argv[1]
    app.run(host='0.0.0.0', port=int(port))
