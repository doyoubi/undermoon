build:
	cargo build

lint:
	find src -name "*.rs" | xargs rustup run stable rustfmt
	cargo clippy

release:
	cargo build --release

server:
	RUST_LOG=undermoon=debug,server_proxy=debug target/debug/server_proxy

coord:
	RUST_LOG=undermoon=debug,coordinator=debug target/debug/coordinator

test_broker:
	RUST_LOG=undermoon=debug,test_http_broker=debug target/debug/test_http_broker

flame:
	sudo flamegraph -o $(name).svg target/release/server_proxy

docker-build-image:
	docker image build -f examples/Dockerfile-undermoon -t undermoon .

docker-multi-redis:
	docker-compose -f examples/docker-compose-multi-redis.yml up

docker-multi-shard:
	docker-compose -f examples/docker-compose-multi-shard.yml up

docker-failover:
	docker-compose -f examples/docker-compose-multi-shard.yml -f examples/docker-compose-failover.yml up

.PHONY: build server coord

