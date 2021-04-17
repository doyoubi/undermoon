import os
import time

from rediscluster import RedisCluster


def create_client(host, port):
    startup_nodes = [{"host": host, "port": port}]
    return RedisCluster(
        startup_nodes=startup_nodes,
        decode_responses=True,
        skip_full_coverage_check=True,
    )


def gen_key(testcase, key):
    return 'goredis:{}:{}:{}'.format(time.time(), testcase, key)


def test_single_key_command(client):
    key = gen_key('singlekey', 'key')
    value = 'singlevalue'

    client.setex(key, 60, value)
    v = client.get(key)
    assert v == value


def test_multi_key_command(client):
    key1 = gen_key('multikey', 'key1:{hashtag}')
    key2 = gen_key('multikey', 'key2:{hashtag}')
    value1 = 'value1'
    value2 = 'value2'

    client.mset({
        key1: value1,
        key2: value2,
    })
    count = client.delete(key1, key2)
    assert count == 2

    values = client.mget(key1, key2)
    assert len(values) == 2
    assert values[0] is None
    assert values[1] is None


if __name__ == '__main__':
    host = os.environ.get('CLIENT_TEST_NODE_HOST') or '127.0.0.1'
    port = os.environ.get('CLIENT_TEST_NODE_PORT') or '5299'
    client = create_client(host, port)

    test_single_key_command(client)
    test_multi_key_command(client)
