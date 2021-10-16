#!/usr/bin/env bash

mkdir -p local_tests/1
mkdir -p local_tests/2

trap "exit" INT TERM
trap "kill 0" EXIT

cd local_tests/1 && redis-server redis.conf --port 7001 &> redis-1.log &
cd local_tests/2 && redis-server redis.conf --port 7002 &> redis-2.log &

proxy="${PWD}/target/release/server_proxy"
conf="${PWD}/conf/server-proxy.toml"

export RUST_LOG=undermoon=info,server_proxy=info
export UNDERMOON_ADDRESS=127.0.0.1:6001 UNDERMOON_ANNOUNCE_ADDRESS=127.0.0.1:6001
cd local_tests/1 && "${proxy}" "${conf}" &> proxy-1.log &
export UNDERMOON_ADDRESS=127.0.0.1:6002 UNDERMOON_ANNOUNCE_ADDRESS=127.0.0.1:6002
cd local_tests/2 && "${proxy}" "${conf}" &> proxy-2.log &

while true; do
    sleep 1

    redis-cli -p 6001 ping
    if [ "$?" == '1' ]; then
        continue
    fi

    redis-cli -p 6002 ping
    if [ "$?" == '1' ]; then
        continue
    fi

    break
done

redis-cli -p 6001 UMCTL SETCLUSTER v2 2 NOFLAGS mydb \
    127.0.0.1:7001 1 0-16383
redis-cli -p 6002 UMCTL SETCLUSTER v2 2 NOFLAGS mydb \
    PEER 127.0.0.1:6001 1 0-16383

wait
