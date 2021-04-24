#!/usr/bin/env sh

redis-cli -h localhost -p 5299 UMCTL SETCLUSTER v2 2 NOFLAGS mydb 127.0.0.1:6379 1 0-16383

