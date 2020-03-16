#!/usr/bin/env bash

function wait_redis() {
    redis-cli -h $1 -p $2 ping
}

declare -a redis_list=('redis1' 'redis2' 'redis3')

for name in "${redis_list[@]}"; do
    until wait_redis ${name} 6379; do
        echo "waiting for ${name}"
        sleep 1
    done
done

until wait_redis server_proxy 5299; do
    echo "waiting for proxy"
    sleep 1
done

redis-cli -h server_proxy -p 5299 UMCTL SETDB 1 FORCE mydb redis1:6379 1 0-5461 mydb redis2:6379 1 5462-10922 mydb redis3:6379 1 10923-16383
