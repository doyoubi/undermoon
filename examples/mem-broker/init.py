import requests

BROKER_API_VERSION = 'v2'

def init_hosts():
    hosts = {
        'server_proxy1:6001': ['redis1:6379', 'redis2:6379'],
        'server_proxy2:6002': ['redis3:6379', 'redis4:6379'],
        'server_proxy3:6003': ['redis5:6379', 'redis6:6379'],
    }
    for proxy_address, node_addresses in hosts.items():
        add_host(proxy_address, node_addresses)


def add_host(proxy_address, node_addresses):
    payload = {
        'proxy_address': proxy_address,
        'nodes': node_addresses,
    }
    res = requests.put('http://localhost:7799/api/{}/proxies/meta'.format(BROKER_API_VERSION), json=payload)
    print(res.status_code, res.text)
    res.raise_for_status()


def add_cluster(cluster_name):
    res = requests.post('http://localhost:7799/api/{}/clusters/{}'.format(BROKER_API_VERSION, cluster_name))
    print(res.status_code, res.text)
    res.raise_for_status()


def add_node(cluster_name):
    res = requests.post('http://localhost:7799/api/{}/clusters/nodes/{}'.format(BROKER_API_VERSION, cluster_name))
    print(res.status_code, res.text)
    res.raise_for_status()


def get_cluster(cluster_name):
    res = requests.get('http://localhost:7799/api/{}/clusters/meta/{}'.format(BROKER_API_VERSION, cluster_name))
    res.raise_for_status()
    return res.json()['cluster']


def migrate_slots(cluster_name):
    cluster = get_cluster(cluster_name)
    nodes = cluster['nodes']
    src = next([n for n in nodes if n['slots']].__iter__(), None)
    dst = next([n for n in nodes if not n['slots'] and n['repl']['role'] == 'master'].__iter__(), None)
    if not src or not dst:
        raise Exception('cannot find src and dst')

    src_address = src['address']
    dst_address = dst['address']
    res = requests.post('http://localhost:7799/api/{}/clusters/{}/migrations/half/{}/{}'.format(BROKER_API_VERSION, cluster_name, src_address, dst_address))
    print(res.status_code, res.text)
    res.raise_for_status()


def set_replica(cluster_name):
    cluster = get_cluster(cluster_name)
    nodes = cluster['nodes']
    master = next([n for n in nodes if n['slots'] and not n['repl']['peers']].__iter__(), None)
    replica = next([n for n in nodes if not n['slots'] and not n['repl']['peers'] and n['proxy_address'] != master['proxy_address']].__iter__(), None)
    if not master or not replica:
        raise Exception('cannot find src and dst')

    master_address = master['address']
    replica_address = replica['address']
    res = requests.post('http://localhost:7799/api/{}/clusters/{}/replications/{}/{}'.format(BROKER_API_VERSION, cluster_name, master_address, replica_address))
    print(res.status_code, res.text)
    res.raise_for_status()


def replace_master_proxy(cluster_name, master=True):
    cluster = get_cluster(cluster_name)
    nodes = cluster['nodes']
    if master:
        node = next([n for n in nodes if n['slots']].__iter__(), None)
    else:
        node = next([n for n in nodes if n['repl']['role'] == 'replica'].__iter__(), None)
    if not node:
        raise Exception('cannot find src and dst')

    proxy_address = node['proxy_address']
    res = requests.post('http://localhost:7799/api/{}/proxies/failover/{}'.format(BROKER_API_VERSION, proxy_address))
    print(res.status_code, res.text)
    res.raise_for_status()


cluster_name = 'mydb'
init_hosts()
add_cluster(cluster_name)
