#!/usr/bin/env bash

TARGET_VOLUMES="${PWD}/examples/target_volume:/undermoon/shared/target"
TOML_VOLUMES="${PWD}/Cargo.toml:/undermoon/Cargo.toml"
LOCK_VOLUMES="${PWD}/Cargo.lock:/undermoon/Cargo.lock"
SRC_VOLUMES="${PWD}/src:/undermoon/src"

docker run --name undermoon_builder_cp_bin --rm -v "${TARGET_VOLUMES}" -v "${TOML_VOLUMES}" -v "${LOCK_VOLUMES}" -v "${SRC_VOLUMES}" undermoon_builder cargo build
docker run --name undermoon_builder_cp_bin --rm -v "${TARGET_VOLUMES}" -v "${TOML_VOLUMES}" -v "${LOCK_VOLUMES}" -v "${SRC_VOLUMES}" undermoon_builder sh /undermoon/copy_target.sh
