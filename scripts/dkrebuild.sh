#!/usr/bin/env bash

TARGET_VOLUME="${PWD}/examples/target_volume:/undermoon/shared/target"
TOML_VOLUME="${PWD}/Cargo.toml:/undermoon/Cargo.toml"
LOCK_VOLUME="${PWD}/Cargo.lock:/undermoon/Cargo.lock"
SRC_VOLUME="${PWD}/src:/undermoon/src"
COPY_SCRIPT_VOLUME="${PWD}/examples/copy_target.sh:/undermoon/copy_target.sh"

docker run --rm -v "${TARGET_VOLUME}" -v "${TOML_VOLUME}" -v "${LOCK_VOLUME}" -v "${SRC_VOLUME}" -v "${COPY_SCRIPT_VOLUME}" undermoon_builder sh /undermoon/copy_target.sh
