import pprint

import requests


pp = pprint.PrettyPrinter(indent=4)


def test_get_cluster_names():
    names = requests.get('http://localhost:5000/api/clusters/names').json()
    pp.pprint(names)

def test_get_cluster():
    res = requests.get('http://localhost:5000/api/clusters/{}/meta'.format('mydb'))
    cluster = res.json()
    pp.pprint(cluster)

def test_get_proxies():
    proxies = requests.get('http://localhost:5000/api/hosts/addresses').json()
    pp.pprint(proxies)

def test_get_proxy(proxy_address):
    proxy = requests.get('http://localhost:5000/api/hosts/addresses/{}'.format(proxy_address)).json()
    pp.pprint(proxy)

def test_report_failure():
    res = requests.post('http://localhost:5000/api/failures/{}/{}'.format('server_proxy1:6001', 'doyoubi'))
    pp.pprint(res.status_code)

def test_get_failures():
    failures = requests.get('http://localhost:5000/api/failures').json()
    pp.pprint(failures)


def test_replace_node():
    payload = {
        "cluster_epoch": 1,
        "node": {
            "address": "redis1:7001",
            "proxy_address": "server_proxy1:6001",
            "cluster_name": "mydb",
            "repl": {
                "role": "master",
                "peers": [{
                    "node_address": "redis5:7005",
                    "proxy_address": "server_proxy2:6002",
                }]
            },
            "slots": [{
                "start": 0,
                "end": 5461,
                "tag": ""
            }]
        }
    }
    new_node = requests.put('http://localhost:5000/api/clusters/nodes', json=payload).json()
    pp.pprint(new_node)

test_get_cluster_names()
test_get_cluster()
test_get_proxies()
test_get_proxy('server_proxy1:6001')
test_get_proxy('server_proxy2:6002')
test_get_proxy('server_proxy3:6003')
test_report_failure()
test_get_failures()
test_replace_node()
