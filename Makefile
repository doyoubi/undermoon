build:
	cargo build

test:
	RUST_BACKTRACE=full cargo test -- --nocapture

install-linters:
	rustup update
	rustup component add clippy
	rustup component add rustfmt
	cargo install --git https://github.com/doyoubi/mylint-rs --tag v1.0

lint:
	find src -name "*.rs" | xargs rustup run stable rustfmt
	find tests -name "*.rs" | xargs rustup run stable rustfmt
	cargo clippy -- -W clippy::indexing_slicing
	mylint -s Expect -s IndexExpression

release:
	cargo build --release

server:
	RUST_LOG=undermoon=debug,server_proxy=debug target/debug/server_proxy conf/server-proxy.toml

server-release:
	RUST_LOG=undermoon=info,server_proxy=info target/release/server_proxy conf/server-proxy.toml

coord:
	RUST_LOG=undermoon=debug,coordinator=debug target/debug/coordinator conf/coordinator.toml

broker:
	RUST_LOG=actix_web=debug,undermoon=debug,mem_broker=debug target/debug/mem_broker conf/mem-broker.toml

broker1:
	RUST_LOG=actix_web=debug,undermoon=debug,mem_broker=debug UNDERMOON_REPLICA_ADDRESSES=127.0.0.1:8899 target/debug/mem_broker conf/mem-broker.toml

broker2:
	RUST_LOG=actix_web=debug,undermoon=debug,mem_broker=debug UNDERMOON_ADDRESS=127.0.0.1:8899 UNDERMOON_META_FILENAME=metadata2 target/debug/mem_broker conf/mem-broker.toml

flame:
	sudo flamegraph -o $(name).svg target/release/server_proxy conf/server-proxy.toml

# Debug image and release image use different ways for building image.
# For faster rebuild, builder image will only build the binaries and move it out
# to the host by shared volume. The debug undermoon image will not get the image
# when being built. Instead we need to specify the volume to `insert` the binary
# to the debug undermoon image.
docker-build-image:
	docker image build -f examples/Dockerfile-builder -t undermoon_builder .
	sh scripts/dkrebuild.sh
	docker image build -f examples/Dockerfile-undermoon -t undermoon .

docker-rebuild-bin:
	sh scripts/dkrebuild.sh

# The release builder will build the binaries and move it out by `docker cp`.
# When the release undermoon image is built, the binaries will be moved into it.
docker-build-release:
	docker image build -f examples/Dockerfile-builder-release -t undermoon_builder_release .
	mkdir -p ./examples/target_volume/release
	docker rm undermoon-builder-container || true
	docker create -it --name undermoon-builder-container undermoon_builder_release bash
	docker cp undermoon-builder-container:/undermoon/target/release/server_proxy ./examples/target_volume/release/
	docker cp undermoon-builder-container:/undermoon/target/release/coordinator ./examples/target_volume/release/
	docker cp undermoon-builder-container:/undermoon/target/release/mem_broker ./examples/target_volume/release/
	docker rm undermoon-builder-container
	docker image build -f examples/Dockerfile-undermoon-release -t undermoon .

docker-mem-broker:
	docker-compose -f examples/docker-compose-mem-broker.yml up

docker-overmoon:
	# Need to build the 'overmoon' image first
	# > git clone https://github.com/doyoubi/overmoon
	# > cd overmoon
	# > make build-docker
	docker-compose -f examples/docker-compose-overmoon.yml up

start-func-test:
	python chaostest/render_compose.py mem_broker
	docker stack deploy --compose-file chaostest/chaos-docker-compose.yml chaos

start-chaos:
	python chaostest/render_compose.py mem_broker enable_failure
	docker stack deploy --compose-file chaostest/chaos-docker-compose.yml chaos

stop-chaos:
	docker stack rm chaos

list-chaos-services:
	docker stack services chaos

chaos-test:
	python chaostest/random_test.py

func-test:
	python chaostest/random_test.py exit-on-error

.PHONY: build test lint release server coord test_broker flame docker-build-image docker-multi-redis docker-multi-shard docker-failover docker-mem-broker docker-overmoon \
    start-func-test start-chaos stop-chaos list-chaos-services chaos-test func-test

