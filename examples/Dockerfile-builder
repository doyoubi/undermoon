FROM rust:latest

WORKDIR /undermoon
COPY src /undermoon/src
COPY Cargo.toml Cargo.lock /undermoon/

RUN cargo build

RUN rm -rf src
RUN rm Cargo.toml Cargo.lock

# Mount new codes into the container.
VOLUME /undermoon/src
VOLUME /undermoon/Cargo.toml /undermoon/Cargo.lock
VOLUME /undermoon/shared/target
VOLUME /undermoon/copy_target.sh
