import random
import redis
import requests
import requests_unixsocket
from loguru import logger


DOCKER_ENDPOINT = 'http+unix://%2Fvar%2Frun%2Fdocker.sock'
OVERMOON_ENDPOINT = 'http://localhost:7799'

BROKER_API_VERSION = 'v2'


class HttpClient:
    def __init__(self, endpoint):
        self.endpoint = endpoint
        if endpoint.startswith('http+unix'):
            self.session = requests_unixsocket.Session()
        else:
            self.session = requests.Session()

    def get(self, path):
        return self.session.get('{}{}'.format(self.endpoint, path))

    def put(self, path, payload=None):
        return self.session.put('{}{}'.format(self.endpoint, path), json=payload)

    def post(self, path, payload=None):
        return self.session.post('{}{}'.format(self.endpoint, path), json=payload)

    def delete(self, path):
        return self.session.delete('{}{}'.format(self.endpoint, path))

    def patch(self, path, payload=None):
        return self.session.patch('{}{}'.format(self.endpoint, path), json=payload)


class DockerHttpClient:
    def __init__(self, http_client, api_version):
        self.http_client = http_client
        self.api_version = api_version

    def get(self, path):
        return self.http_client.get('/{}{}'.format(self.api_version, path))

    def put(self, path, payload=None):
        return self.http_client.put('/{}{}'.format(self.api_version, path), payload)

    def post(self, path, payload=None):
        return self.http_client.post('/{}{}'.format(self.api_version, path), payload)

    def delete(self, path):
        return self.http_client.delete('/{}{}'.format(self.api_version, path))


class ServerProxy:
    def __init__(self, proxy_address, nodes):
        self.proxy_address = proxy_address
        self.nodes = nodes

    def to_dict(self):
        return {
            'proxy_address': self.proxy_address,
            'nodes': self.nodes,
        }


class OvermoonClient:
    def __init__(self, overmoon_endpoint):
        self.overmoon_endpoint = overmoon_endpoint
        self.client = HttpClient(overmoon_endpoint)

    def sync_server_proxy(self, server_proxy):
        server_proxy_port = int(server_proxy.proxy_address.split(':')[1])

        conn_timeout = 1
        redis_client = redis.StrictRedis(
            port=server_proxy_port,
            socket_timeout=conn_timeout,
            socket_connect_timeout=conn_timeout,
        )
        try:
            redis_client.ping()
        except:
            logger.warning('OVERMOON: failed to connect to server proxy: {}', server_proxy.to_dict())
            return

        r = self.client.post('/api/{}/proxies/meta'.format(BROKER_API_VERSION), server_proxy.to_dict())
        if r.status_code == 400:
            return
        if r.status_code == 200:
            logger.info('OVERMOON: recover server proxy: {}', server_proxy.to_dict())
            return

        logger.error('OVERMOON_ERROR: failed to sync server proxy data: {} {}', r.status_code, r.text)

    def sync_all_server_proxy(self, server_proxy_list):
        for server_proxy in server_proxy_list:
            try:
                self.sync_server_proxy(server_proxy)
            except Exception as e:
                logger.error('OVERMOON: sync_server_proxy failed: {} {}', server_proxy.to_dict(), e)

    def create_cluster(self, cluster_name, node_number):
        payload = {
            'node_number': node_number,
        }
        r = self.client.post('/api/{}/clusters/meta/{}'.format(BROKER_API_VERSION, cluster_name), payload)
        if r.status_code == 200:
            logger.warning('created cluster: {} {}', cluster_name, node_number)
            return
        if r.status_code == 400:
            return
        if r.status_code == 409:
            logger.warning('no resource')
            return

        logger.error('OVERMOON_ERROR: failed to create cluster: {} {}', r.status_code, r.text)

    def get_cluster_names(self):
        r = self.client.get('/api/{}/clusters/names'.format(BROKER_API_VERSION))
        if r.status_code != 200:
            logger.error('OVERMOON_ERROR: failed to get cluster names: {} {}'.format(r.status_code, r.text))
            return

        payload = r.json()
        names = payload['names']
        return names

    def get_cluster(self, cluster_name):
        r = self.client.get('/api/{}/clusters/meta/{}'.format(BROKER_API_VERSION, cluster_name))
        if r.status_code == 200:
            return r.json()['cluster']
        logger.error('OVERMOON_ERROR: failed to get cluster: {} {} {}', cluster_name, r.status_code, r.text)

    def delete_cluster(self, cluster_name):
        r = self.client.delete('/api/{}/clusters/meta/{}'.format(BROKER_API_VERSION, cluster_name))
        if r.status_code == 200:
            logger.warning('deleted cluster: {}', cluster_name)
            return
        if r.status_code == 404:
            return
        logger.error('OVERMOON_ERROR: failed to delete cluster: {} {} {}', cluster_name, r.status_code, r.text)

    def add_nodes(self, cluster_name, node_number):
        payload = {
            'node_number': node_number,
        }
        r = self.client.patch('/api/{}/clusters/nodes/{}'.format(BROKER_API_VERSION, cluster_name), payload)
        if r.status_code == 200:
            logger.info('added nodes to cluster {}', cluster_name)
            return
        if r.status_code in (400, 404, 409):
            return
        logger.error('OVERMOON_ERROR: failed to add nodes: {} {} {}', cluster_name, r.status_code, r.text)

    def remove_unused_nodes(self, cluster_name):
        r = self.client.delete('/api/{}/clusters/free_nodes/{}'.format(BROKER_API_VERSION, cluster_name))
        if r.status_code == 200:
            logger.info('OVERMOON removed unused nodes')
            return True
        if r.status_code in (404, 409):
            return False
        if r.status_code == 400:
            # logger.warning('OVERMOON failed to remove unused nodes: {} {} {}', cluster_name, r.status_code, r.text)
            return False

        logger.error('OVERMOON_ERROR: failed to remove unused nodes: {} {} {}: {}', cluster_name, r.status_code, r.text, self.get_cluster(cluster_name))
        return False

    def scale_cluster(self, cluster_name):
        r = self.client.post('/api/{}/clusters/migrations/{}'.format(BROKER_API_VERSION, cluster_name))
        if r.status_code == 200:
            logger.warning('start migration: {}', cluster_name)
            return
        if r.status_code in (400, 404, 409):
            return

        logger.error('OVERMOON_ERROR: failed to start migration: {} {} {}: {}', cluster_name, r.status_code, r.text, self.get_cluster(cluster_name))

    def get_proxy_addresses(self):
        r = self.client.get('/api/{}/proxies/addresses'.format(BROKER_API_VERSION))
        if r.status_code == 200:
            return r.json()['addresses']
        logger.error('OVERMOON_ERROR: failed to get proxy addresses: {} {}', r.status_code, r.text)
        return []

    def get_proxy(self, address):
        r = self.client.get('/api/{}/proxies/meta/{}'.format(BROKER_API_VERSION, address))
        if r.status_code == 200:
            return r.json()['proxy']
        logger.error('OVERMOON_ERROR: failed to get proxy meta: {} {}', r.status_code, r.text)
        return None

    def get_failures(self):
        r = self.client.get('/api/{}/failures'.format(BROKER_API_VERSION))
        if r.status_code == 200:
            return r.json()['addresses']
        logger.error('OVERMOON_ERROR: failed to get failed proxy addresses: {} {}', r.status_code, r.text)
        return []

    def get_free_proxies(self):
        proxies = self.get_proxy_addresses()
        failures = self.get_failures()
        failures = set(failures)

        free_proxies = []
        for addr in proxies:
            if addr in failures:
                continue
            proxy = self.get_proxy(addr)
            if not proxy:
                continue
            if not proxy['free_nodes']:
                continue
            free_proxies.append(addr)
        return free_proxies


class RedisClusterClient:
    def __init__(self, startup_nodes, timeout):
        self.startup_nodes = startup_nodes
        self.client_map = {}
        self.timeout = timeout
        for proxy in startup_nodes:
            self.get_or_create_client(proxy)

    def get_or_create_client(self, proxy):
        host = proxy['host']
        port = proxy['port']
        address = self.fmt_addr(proxy)
        if address not in self.client_map:
            self.client_map[address] = redis.StrictRedis(host, port, socket_timeout=self.timeout, socket_connect_timeout=self.timeout)
        return self.client_map[address]

    def get(self, key):
        return self.exec(lambda client: self.get_helper(client, key))

    def get_helper(self, client, key):
        v = client.get(key)
        if v:
            v = v.decode('utf-8')
        return v

    def set(self, key, value):
        return self.exec(lambda client: client.set(key, value))

    def exec(self, send_func):
        proxy = random.choice(self.startup_nodes)
        client = self.get_or_create_client(proxy)

        # Cover this case:
        # (1) random node
        # (2) importing node (PreSwitched not done)
        # (3) migrating node (PreSwitched done this time)
        # (4) importing node again!
        RETRY_TIMES = 4
        tried_addrs = [self.fmt_addr(proxy)]
        for i in range(0, RETRY_TIMES):
            try:
                addr = self.fmt_addr(proxy)
                return (send_func(client), addr)
            except Exception as e:
                if 'MOVED' not in str(e):
                    raise Exception('{}: {}'.format(addr, e))
                if i == RETRY_TIMES - 1:
                    logger.error("exceed max redirection times: {}", tried_addrs)
                    raise Exception("{}: {}".format(addr, e))
                proxy = self.parse_moved(str(e))
                client = self.get_or_create_client(proxy)
                tried_addrs.append(self.fmt_addr(proxy))

    def parse_moved(self, response):
        segs = response.split(' ')
        if len(segs) != 3:
            raise Exception("invalid moved response {}".format(response))
        addr = segs[2]
        addr_segs = addr.split(':')
        if len(addr_segs) != 2:
            raise Exception("invalid moved response {}".format(response))
        return {'host': addr_segs[0], 'port': addr_segs[1]}

    def fmt_addr(self, proxy):
        return '{}:{}'.format(proxy['host'],  proxy['port'])
