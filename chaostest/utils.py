import redis
import requests
import requests_unixsocket


DOCKER_ENDPOINT = 'http+unix://%2Fvar%2Frun%2Fdocker.sock'
OVERMOON_ENDPOINT = 'http://localhost:7799'


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


class DockerHttpClient:
    def __init__(self, http_client, api_version):
        self.http_client = http_client
        self.api_version = api_version

    def get(self, path):
        return self.http_client.get('/{}{}'.format(self.api_version, path))

    def put(self, path, payload=None):
        return self.http_client.put('/{}{}'.format(self.api_version, path), payload)

    def post(self, path, payload=None):
        return self.http_client.post('{}{}'.format(self.api_version, path), payload)

    def delete(self, path):
        return self.http_client.delete('{}{}'.format(self.api_version, path))


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

        conn_timeout = 0.2
        redis_client = redis.StrictRedis(
            port=server_proxy_port,
            socket_timeout=conn_timeout,
            socket_connect_timeout=conn_timeout,
        )
        try:
            redis_client.ping()
        except:
            print('failed to connect to server proxy: {}'.format(server_proxy.to_dict()))
            return

        r = self.client.post('/api/proxies/nodes', server_proxy.to_dict())
        if r.status_code == 400:
            return
        if r.status_code == 200:
            print('recover server proxy: {}'.format(server_proxy))
            return

        print('OVERMOON_ERROR: failed to sync server proxy data: {} {}'.format(r.status_code, r.text))

    def sync_all_server_proxy(self, server_proxy_list):
        for server_proxy in server_proxy_list:
            try:
                self.sync_server_proxy(server_proxy)
            except:
                print('sync_server_proxy failed: {}'.format(server_proxy))

    def create_cluster(self, cluster_name, node_number):
        payload = {
            'cluster_name': cluster_name,
            'node_number': node_number,
        }
        r = self.client.post('/api/clusters', payload)
        if r.status_code == 200:
            print('created cluster: {} {}'.format(cluster_name, node_number))
            return
        if r.status_code == 400:
            print('failed to create cluster:', r.text)
            return
        if r.status_code == 409:
            print('no resource')
            return

        print('OVERMOON_ERROR: failed to create cluster: {} {}'.format(r.status_code, r.text))

    def get_cluster_names(self):
        r = self.client.get('/api/clusters/names')
        if r.status_code != 200:
            print('OVERMOON_ERROR: failed to get cluster names: {} {}'.format(r.status_code, r.text))
            return

        payload = r.json()
        names = payload['names']
        return names

    def delete_cluster(self, cluster_name):
        r = self.client.delete('/api/clusters/meta/{}'.format(cluster_name))
        if r.status_code == 200:
            print('deleted cluster: {}'.format(cluster_name))
            return
        if r.status_code == 404:
            return
        print('OVERMOON_ERROR: failed to delete cluster: {} {} {}'.format(cluster_name, r.status_code, r.text))
