#!/usr/bin/env bash
docker ps | grep undermoon | awk '{print $1}' | xargs -I{} docker exec {} cargo build
