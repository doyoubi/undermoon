import requests

def init_hosts():
    hosts = {
        'server_proxy1:6001': ['redis1:6379', 'redis2:6379'],
        'server_proxy2:6002': ['redis3:6379', 'redis4:6379'],
        'server_proxy3:6003': ['redis5:6379', 'redis6:6379'],
        'server_proxy4:6004': ['redis7:6379', 'redis8:6379'],
        'server_proxy5:6005': ['redis9:6379', 'redis10:6379'],
        'server_proxy6:6006': ['redis11:6379', 'redis12:6379'],
    }
    for proxy_address, node_addresses in hosts.items():
        add_host(proxy_address, node_addresses)


def add_host(proxy_address, node_addresses):
    payload = {
        'proxy_address': proxy_address,
        'nodes': node_addresses,
    }
    res = requests.post('http://localhost:7799/api/proxies/nodes', json=payload)
    print(res.status_code, res.text)
    if res.status_code == 400:
        return
    res.raise_for_status()


def add_cluster(cluster_name):
    payload = {
        'cluster_name': cluster_name,
        'node_number': 4,
    }
    res = requests.post('http://localhost:7799/api/clusters', json=payload)
    print(res.status_code, res.text)
    res.raise_for_status()


def add_node(cluster_name):
    payload = {
        'expected_node_number': 8
    }
    res = requests.put('http://localhost:7799/api/clusters/nodes/{}'.format(cluster_name), json=payload)
    print(res.status_code, res.text)
    res.raise_for_status()


def get_cluster(cluster_name):
    res = requests.get('http://localhost:7799/api/clusters/meta/{}'.format(cluster_name))
    res.raise_for_status()
    return res.json()['cluster']


def migrate_slots(cluster_name):
    res = requests.post('http://localhost:7799/api/clusters/migrations/{}'.format(cluster_name))
    print(res.status_code, res.text)
    res.raise_for_status()


def replace_proxy(cluster_name, master=True):
    cluster = get_cluster(cluster_name)
    nodes = cluster['nodes']
    if master:
        node = next([n for n in nodes if n['slots']].__iter__(), None)
    else:
        node = next([n for n in nodes if n['repl']['role'] == 'replica'].__iter__(), None)
    if not node:
        raise Exception('cannot find src and dst')

    proxy_address = node['proxy_address']
    res = requests.post('http://localhost:7799/api/proxies/failover/{}'.format(proxy_address))
    print(res.status_code, res.text)
    res.raise_for_status()


cluster_name = 'mydb'
init_hosts()
add_cluster(cluster_name)
add_node(cluster_name)
print(get_cluster(cluster_name))

migrate_slots(cluster_name)

replace_proxy(cluster_name)
replace_proxy(cluster_name, False)
print(get_cluster(cluster_name))
