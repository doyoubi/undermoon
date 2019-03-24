build:
	cargo build

release:
	cargo build --release

server:
	RUST_LOG=undermoon=debug,server_proxy=debug target/debug/server_proxy

coord:
	RUST_LOG=undermoon=debug,coordinator=debug target/debug/coordinator

flame:
	sudo flamegraph -o $(name).svg target/release/server_proxy

.PHONY: build server coord
