#!/usr/bin/env bash
cargo build
mkdir -p /undermoon/shared/target/debug
cp /undermoon/target/debug/coordinator /undermoon/shared/target/debug
cp /undermoon/target/debug/server_proxy /undermoon/shared/target/debug
cp /undermoon/target/debug/mem_broker /undermoon/shared/target/debug
