build:
	cargo build

test:
	RUST_BACKTRACE=full cargo test -- --nocapture

install-linters:
	rustup update
	rustup component add clippy
	rustup component add rustfmt

lint:
	cargo fmt --all
	cargo clippy

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

# Image for testing undermoon-operator
docker-build-test-image:
	docker image build -f examples/Dockerfile-undermoon-test -t undermoon_test .

docker-build-release:
	docker image build -f examples/Dockerfile-undermoon-release -t undermoon .

docker-mem-broker:
	docker-compose -f examples/docker-compose-mem-broker.yml up

docker-mem-broker-example:
	docker-compose -f examples/docker-compose-mem-broker-example.yml up

start-func-test:
	python chaostest/render_compose.py -t mem_broker
	docker stack deploy --compose-file chaostest/chaos-docker-compose.yml chaos

start-func-test-active:
	python chaostest/render_compose.py -t mem_broker -a
	docker stack deploy --compose-file chaostest/chaos-docker-compose.yml chaos

start-chaos:
	python chaostest/render_compose.py -t mem_broker -f
	docker stack deploy --compose-file chaostest/chaos-docker-compose.yml chaos

start-chaos-active:
	python chaostest/render_compose.py -t mem_broker -f -a
	docker stack deploy --compose-file chaostest/chaos-docker-compose.yml chaos

stop-chaos:
	docker stack rm chaos

list-chaos-services:
	docker stack services chaos

chaos-test:
	python chaostest/random_test.py

func-test:
	python chaostest/random_test.py exit-on-error

.PHONY: build test lint release server coord test_broker flame docker-multi-redis docker-multi-shard docker-failover docker-mem-broker \
    start-func-test start-chaos stop-chaos list-chaos-services chaos-test func-test

