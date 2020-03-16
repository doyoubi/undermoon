import sys
import time
import logging
from copy import deepcopy

import redis


logger = logging.getLogger('checker')


DB_NAME = 'mydb'
INIT_SLOTS_CONFIG = {
    'server_proxy1:6001': [[0, 5461]],
    'server_proxy2:6002': [[5462, 10922]],
    'server_proxy3:6003': [[10923, 16383]],
}
INIT_NODES_CONFIG = {
    'server_proxy1:6001': 'redis1:6379',
    'server_proxy2:6002': 'redis2:6379',
    'server_proxy3:6003': 'redis3:6379',
}


def init_logger():
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)


def check_all_nodes(init_config):
    nodes = init_config.keys()
    return [n for n in nodes if not check_alive(n)]


def check_alive(address):
    host, port = address.split(':')
    try:
        redis.StrictRedis(host, port, socket_timeout=1).ping()
        return True
    except redis.RedisError as e:
        logger.error('failed to connect to redis: %s', e)
        return False


def gen_final_config(failed_nodes, init_slots_config):
    missing_slots = sum(
        [slots for node, slots in init_slots_config.items()
         if node in failed_nodes],
        [])

    config = {node: slots for node, slots in init_slots_config.items()
              if node not in failed_nodes}

    while missing_slots and config.keys():
        for node in config.keys():
            if not missing_slots:
                break
            config[node].append(missing_slots.pop())

    return config


def send_all_config(slots_config, nodes_config):
    for node, slots in slots_config.items():
        try:
            send_config(node, slots_config, nodes_config)
        except redis.RedisError as e:
            logger.error('failed to send config %s', e)


def send_config(address, slots_config, nodes_config):
    assert address in slots_config
    host, port = address.split(':')
    client = redis.StrictRedis(host, port, socket_timeout=1)

    setdb = ['UMCTL', 'SETDB', '1', 'FORCE']
    for start, end in slots_config[address]:
        setdb.extend([DB_NAME, nodes_config[address], '1', '{}-{}'.format(start, end)])

    setdb.append('PEER')
    for node, slots in slots_config.items():
        if node == address:
            continue
        for start, end in slots:
            setdb.extend([DB_NAME, node, '1', '{}-{}'.format(start, end)])

    logger.info('sending setdb: %s', setdb)
    client.execute_command(*setdb)


def run_checker(init_nodes_config, init_slots_config):
    init_logger()
    logger.info("start checker")
    while True:
        time.sleep(5)
        failed_nodes = check_all_nodes(init_nodes_config)
        slot_config = gen_final_config(failed_nodes, deepcopy(init_slots_config))
        send_all_config(slot_config, init_nodes_config)


if '__main__' == __name__:
    run_checker(INIT_NODES_CONFIG, INIT_SLOTS_CONFIG)
