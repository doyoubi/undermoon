#!/usr/bin/env bash
random_time=$(date +%s)

for i in {0..9}
do
    redis-cli -h server_proxy1 -p 6001 -a mydb -c SET "test:${random_time}:${i}" "test:${random_time}:${i}"
done

function test_value() {
    for i in {0..9}
    do
        key="test:${random_time}:${i}"
        value=$(redis-cli -h server_proxy2 -p 6002 -a mydb -c GET ${key})
        [ "$value" != "$key" ] && echo "wrong value ${value}, expecting ${key}"
    done
}

echo '############################'
echo 'testing after setting values'
echo '############################'
test_value

echo '############################'
echo 'start to migrate'
echo '############################'
curl -XPOST localhost:6699/api/test/migration

for i in {0..6}; do
    redis-cli -h server_proxy2 -p 6002 -a mydb -c cluster nodes
    test_value
    sleep 1
done
