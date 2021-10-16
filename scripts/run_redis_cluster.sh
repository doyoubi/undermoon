#!/usr/bin/env bash

rm -f local_tests/redis_cluster/1/dump.rdb
rm -f local_tests/redis_cluster/2/dump.rdb

trap "exit" INT TERM
trap "kill 0" EXIT

migrating_nodes_conf="migrating_redis_________________________ 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-16383
importing_redis_________________________ 127.0.0.1:7002@17002 master - 0 0 1 connected
vars currentEpoch 1 lastVoteEpoch 1"
importing_nodes_conf="migrating_redis_________________________ 127.0.0.1:7001@17001 master - 0 0 1 connected 0-16383
importing_redis_________________________ 127.0.0.1:7002@17002 myself,master - 0 0 1 connected
vars currentEpoch 1 lastVoteEpoch 1"

echo "${migrating_nodes_conf}" > local_tests/redis_cluster/1/nodes.conf
echo "${importing_nodes_conf}" > local_tests/redis_cluster/2/nodes.conf

cd local_tests/redis_cluster/1 && redis-server redis.conf --port 7001 &> redis-1.log &
cd local_tests/redis_cluster/2 && redis-server redis.conf --port 7002 &> redis-2.log &

while true; do
    sleep 1

    redis-cli -p 7001 ping
    if [ "$?" == '1' ]; then
        continue
    fi

    redis-cli -p 7002 ping
    if [ "$?" == '1' ]; then
        continue
    fi

    break
done

echo 'redis cluster is ready'

wait
