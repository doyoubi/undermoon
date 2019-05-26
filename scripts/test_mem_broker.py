import pprint

import requests


pp = pprint.PrettyPrinter(indent=2)


def get_all_meta():
    res = requests.get('http://localhost:7799/api/metadata')
    pp.pprint(res.json())
    res.raise_for_status()


def test_adding_host(proxy, nodes):
    payload = {
        'proxy_address': proxy,
        'nodes': nodes,
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


def assign_replica():
    cluster_name = 'testdb'
    master_address = '127.0.0.1:6000'
    replica_address = '127.0.0.2:6002'
    res = requests.post('http://localhost:7799/api/replications/{}/{}/{}'.format(cluster_name, master_address, replica_address))
    print(res.status_code, res.text)
    res.raise_for_status()


def get_cluster_names():
    res = requests.get('http://localhost:7799/api/clusters/names')
    print(res.status_code, res.text)
    res.raise_for_status()


def get_cluster_by_name():
    cluster_name = 'testdb'
    res = requests.get('http://localhost:7799/api/clusters/name/{}'.format(cluster_name))
    print(res.status_code, res.text)
    res.raise_for_status()
    return res.json()['cluster']


def get_host_addresses():
    res = requests.get('http://localhost:7799/api/hosts/addresses')
    print(res.status_code, res.text)
    res.raise_for_status()


def get_host_by_address():
    address = '127.0.0.1:7000'
    res = requests.get('http://localhost:7799/api/hosts/address/{}'.format(address))
    print(res.status_code, res.text)
    res.raise_for_status()


def test_adding_failures():
    proxy_address = '127.0.0.1:7000'
    node_address = '127.0.0.1:6000'
    res = requests.post('http://localhost:7799/api/failures/{}/{}'.format(proxy_address, node_address))
    print(res.status_code, res.text)
    res.raise_for_status()


def test_getting_failures():
    res = requests.get('http://localhost:7799/api/failures')
    print(res.status_code, res.text)
    res.raise_for_status()


def test_replacing_master_node():
    cluster = get_cluster_by_name()
    nodes = cluster['nodes']
    master = next([n for n in nodes if n['repl']['role'] == 'master' and n['slots']].__iter__(), None)
    payload = {
        'cluster_epoch': cluster['epoch'],
        'node': master,
    }
    res = requests.put('http://localhost:7799/api/clusters/nodes', json=payload)
    print(res.status_code, res.text)
    res.raise_for_status()


def test_replacing_replica_node():
    cluster = get_cluster_by_name()
    nodes = cluster['nodes']
    replica = next([n for n in nodes if n['repl']['role'] == 'replica'].__iter__(), None)
    payload = {
        'cluster_epoch': cluster['epoch'],
        'node': replica,
    }
    res = requests.put('http://localhost:7799/api/clusters/nodes', json=payload)
    print(res.status_code, res.text)
    res.raise_for_status()


print('add_host')
test_adding_host('127.0.0.1:7000', ['127.0.0.1:6000'])
print('add_cluster')
test_adding_cluster()
get_all_meta()
print('add_node')
test_adding_host('127.0.0.1:7000', ['127.0.0.1:6000', '127.0.0.1:6001'])
test_adding_node()
get_all_meta()
print('remove_node_from_cluster')
test_removing_node_from_cluster()
get_all_meta()
print('remove_node')
test_removing_node()
get_all_meta()
print('migrate_half')
test_adding_host('127.0.0.1:7000', ['127.0.0.1:6000', '127.0.0.1:6001'])
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
print('stop_migrations')
stop_migrations()
get_all_meta()
print('assign_replica')
test_adding_host('127.0.0.2:7000', ['127.0.0.2:6002'])
test_adding_node()
assign_replica()
get_all_meta()
print('get_cluster_names')
get_cluster_names()
print('get_cluster_by_name')
get_cluster_by_name()
print('get_host_addresses')
get_host_addresses()
print('get_host_by_address')
get_host_by_address()
print('test_add_failures')
test_adding_failures()
get_all_meta()
print('test_getting_failures')
test_getting_failures()
get_all_meta()
print('test_replacing_master_node')
test_replacing_master_node()
get_all_meta()
print('test_replacing_replica_node')
test_adding_host('127.0.0.2:7000', ['127.0.0.2:6003'])
test_replacing_replica_node()
get_all_meta()
print('remove cluster')
test_removing_cluster()
