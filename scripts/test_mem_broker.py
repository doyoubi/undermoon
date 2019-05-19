import pprint

import requests


pp = pprint.PrettyPrinter(indent=2)


def get_all_meta():
    res = requests.get('http://localhost:7799/api/metadata')
    pp.pprint(res.json())
    res.raise_for_status()


def test_adding_host():
    payload = {
        'proxy_address': '127.0.0.1:7000',
        'nodes': ['127.0.0.1:6000', '127.0.0.1:6001'],
    }
    res = requests.put('http://localhost:7799/api/hosts/nodes', json=payload)
    print(res.status_code, res.text)
    res.raise_for_status()


def test_adding_cluster():
    cluster_name = 'testdb'
    res = requests.post('http://localhost:7799/api/clusters/{}'.format(cluster_name))
    print(res.status_code, res.text)
    res.raise_for_status()


def test_removing_cluster():
    cluster_name = 'testdb'
    res = requests.delete('http://localhost:7799/api/clusters/{}'.format(cluster_name))
    print(res.status_code, res.text)
    res.raise_for_status()


def test_adding_node():
    cluster_name = 'testdb'
    res = requests.post('http://localhost:7799/api/clusters/{}/nodes'.format(cluster_name))
    print(res.status_code, res.text)
    res.raise_for_status()


def test_removing_node():
    cluster_name = 'testdb'
    proxy_address = '127.0.0.1:7000'
    node_address = '127.0.0.1:6001'
    res = requests.delete('http://localhost:7799/api/clusters/{}/nodes/{}/{}'.format(cluster_name, proxy_address, node_address))
    print(res.status_code, res.text)
    res.raise_for_status()

print('add_host')
test_adding_host()
print('add_cluster')
test_adding_cluster()
get_all_meta()
print('add_node')
test_adding_node()
get_all_meta()
print('remove_node')
test_removing_node()
get_all_meta()
print('remove cluster')
test_removing_cluster()
