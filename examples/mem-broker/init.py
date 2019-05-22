import requests

def init_hosts():
    hosts = {
        'server_proxy1:6001': ['redis1:6379', 'redis2:6379'],
        'server_proxy2:6002': ['redis2:6379', 'redis4:6379'],
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
    res = requests.put('http://localhost:7799/api/hosts/nodes', json=payload)
    print(res.status_code, res.text)
    res.raise_for_status()


def add_cluster(cluster_name):
    res = requests.post('http://localhost:7799/api/clusters/{}'.format(cluster_name))
    print(res.status_code, res.text)
    res.raise_for_status()


def add_node(cluster_name):
    res = requests.post('http://localhost:7799/api/clusters/{}/nodes'.format(cluster_name))
    print(res.status_code, res.text)
    res.raise_for_status()


cluster_name = 'mydb'
init_hosts()
add_cluster(cluster_name)
add_node(cluster_name)
