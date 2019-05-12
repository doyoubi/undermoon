import time

from rediscluster import StrictRedisCluster

startup_nodes = [{"host": "127.0.0.1", "port": "6001"}]
rc = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True, password='mydb', skip_full_coverage_check=True)

t = int(time.time())

for i in range(0, 100000000):
    res = rc.get('test:{}:{}'.format(t, i))
    assert res is None

