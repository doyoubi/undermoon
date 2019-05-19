import pprint

import requests


pp = pprint.PrettyPrinter(indent=2)


def get_all_meta():
    res = requests.get('http://localhost:7799/api/metadata')
    pp.pprint(res.json())
    res.raise_for_status()


def test_adding_host(add_all=False):
    payload = {
        'proxy_address': '127.0.0.1:7000',
        'nodes': ['127.0.0.1:6000', '127.0.0.1:6001'] if add_all else ['127.0.0.1:6000'],
    }
    res = requests.put('http://localhost:7799/api/hosts/nodes', json=payload)
    print(res.status_code, res.text)
    res.raise_for_status()


def test_removing_node():
    proxy_address = '127.0.0.1:7000'
    node_address = '127.0.0.1:6001'
    res = requests.delete('http://localhost:7799/api/hosts/nodes/{}/{}'.format(proxy_address, node_address))
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


def test_removing_node_from_cluster():
    cluster_name = 'testdb'
    proxy_address = '127.0.0.1:7000'
    node_address = '127.0.0.1:6001'
    res = requests.delete('http://localhost:7799/api/clusters/{}/nodes/{}/{}'.format(cluster_name, proxy_address, node_address))
    print(res.status_code, res.text)
    res.raise_for_status()


def migrate_half():
    cluster_name = 'testdb'
    src_address = '127.0.0.1:6000'
    dst_address = '127.0.0.1:6001'
    res = requests.post('http://localhost:7799/api/migrations/half/{}/{}/{}'.format(cluster_name, src_address, dst_address))
    print(res.status_code, res.text)
    res.raise_for_status()


def migrate_all():
    cluster_name = 'testdb'
    src_address = '127.0.0.1:6000'
    dst_address = '127.0.0.1:6001'
    res = requests.post('http://localhost:7799/api/migrations/all/{}/{}/{}'.format(cluster_name, src_address, dst_address))
    print(res.status_code, res.text)
    res.raise_for_status()


def stop_migrations():
    cluster_name = 'testdb'
    src_address = '127.0.0.1:6000'
    dst_address = '127.0.0.1:6001'
    res = requests.delete('http://localhost:7799/api/migrations/{}/{}/{}'.format(cluster_name, src_address, dst_address))
    print(res.status_code, res.text)
    res.raise_for_status()


print('add_host')
test_adding_host()
print('add_cluster')
test_adding_cluster()
get_all_meta()
print('add_node')
test_adding_host(True)
test_adding_node()
get_all_meta()
print('remove_node_from_cluster')
test_removing_node_from_cluster()
get_all_meta()
print('remove_node')
test_removing_node()
get_all_meta()
print('migrate_half')
test_adding_host(True)
test_adding_node()
get_all_meta()
migrate_half()
get_all_meta()
print('stop_migrations')
stop_migrations()
get_all_meta()
print('migrate_all')
migrate_all()
get_all_meta()
# print('stop_migrations')
# stop_migrations()
# get_all_meta()
print('remove cluster')
test_removing_cluster()
