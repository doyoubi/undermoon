#!/usr/bin/env bash

function wait_for_migration() {
    while true; do
        sleep 1

        local r
        r=$(redis-cli -p 6001 UMCTL INFOMGR)
        if [ "${r}" == '' ]; then
            continue
        fi
        echo "Migration: ${r}"

        r=$(redis-cli -p 6002 UMCTL INFOMGR)
        if [ "${r}" == '' ]; then
            continue
        fi
        echo "Migration: ${r}"

        return
    done
}

function get_epoch() {
    local epoch
    epoch=$(redis-cli -p 6001 UMCTL GETEPOCH)
    local epoch2
    epoch2=$(redis-cli -p 6002 UMCTL GETEPOCH)
    if [ "${epoch2}" -lt "${epoch}" ]; then
        epoch="${epoch2}"
    fi
    echo "${epoch}"
}

expand=true

while true; do
    epoch=$(get_epoch)
    epoch=$((epoch+1))
    echo "Start to scale. Epoch: ${epoch}"
    date

    if [ "${expand}" = true ]; then
        echo 'start scaling out'
        expand=false

        redis-cli -p 6001 UMCTL SETCLUSTER v2 "${epoch}" NOFLAGS mydb \
            127.0.0.1:7001 1 0-8000 \
            127.0.0.1:7001 migrating 1 8001-16383 "${epoch}" 127.0.0.1:6001 127.0.0.1:7001 127.0.0.1:6002 127.0.0.1:7002 \
            PEER 127.0.0.1:6002 importing 1 8001-16383 "${epoch}" 127.0.0.1:6001 127.0.0.1:7001 127.0.0.1:6002 127.0.0.1:7002
        redis-cli -p 6002 UMCTL SETCLUSTER v2 "${epoch}" NOFLAGS mydb \
            127.0.0.1:7002 importing 1 8001-16383 "${epoch}" 127.0.0.1:6001 127.0.0.1:7001 127.0.0.1:6002 127.0.0.1:7002 \
            PEER 127.0.0.1:6001 1 0-8000 \
            127.0.0.1:6001 migrating 1 8001-16383 "${epoch}" 127.0.0.1:6001 127.0.0.1:7001 127.0.0.1:6002 127.0.0.1:7002

        wait_for_migration
        epoch=$(get_epoch)
        epoch=$((epoch+1))
        echo "Start to commit. Epoch: ${epoch}"

        redis-cli -p 6001 UMCTL SETCLUSTER v2 "${epoch}" noflags mydb \
            127.0.0.1:7001 1 0-8000 \
            PEER 127.0.0.1:6002 1 8001-16383
        redis-cli -p 6002 UMCTL SETCLUSTER v2 "${epoch}" noflags mydb \
            127.0.0.1:7002 1 8001-16383 \
            PEER 127.0.0.1:6001 1 0-8000
    else
        echo 'start scaling down'
        expand=true

        redis-cli -p 6001 UMCTL SETCLUSTER v2 "${epoch}" NOFLAGS mydb \
            127.0.0.1:7001 1 0-8000 \
            127.0.0.1:7001 importing 1 8001-16383 "${epoch}" 127.0.0.1:6002 127.0.0.1:7002 127.0.0.1:6001 127.0.0.1:7001 \
            PEER 127.0.0.1:6002 migrating 1 8001-16383 "${epoch}" 127.0.0.1:6002 127.0.0.1:7002 127.0.0.1:6001 127.0.0.1:7001
        redis-cli -p 6002 UMCTL SETCLUSTER v2 "${epoch}" NOFLAGS mydb \
            127.0.0.1:7002 migrating 1 8001-16383 "${epoch}" 127.0.0.1:6002 127.0.0.1:7002 127.0.0.1:6001 127.0.0.1:7001 \
            PEER 127.0.0.1:6001 1 0-8000 \
            127.0.0.1:6001 importing 1 8001-16383 "${epoch}" 127.0.0.1:6002 127.0.0.1:7002 127.0.0.1:6001 127.0.0.1:7001

        wait_for_migration
        epoch=$(get_epoch)
        epoch=$((epoch+1))
        echo "Start to commit. Epoch: ${epoch}"

        redis-cli -p 6001 UMCTL SETCLUSTER v2 "${epoch}" noflags mydb \
            127.0.0.1:7001 1 0-16383
        redis-cli -p 6002 UMCTL SETCLUSTER v2 "${epoch}" noflags mydb \
            PEER 127.0.0.1:6001 1 0-16383
    fi

    sleep 3
done
