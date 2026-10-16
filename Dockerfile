# Built and tested on Ubuntu 24.04, whose gRPC 1.51 package ships the CMake
# config files this project's CMakeLists.txt relies on. (22.04's older gRPC
# package has not been tried.)
#
# Single stage on purpose: easy to read, and the shared libraries the binaries
# need are already installed. (A multi-stage build that copies only
# /usr/local/bin/raft_* plus the runtime libraries is a size optimisation.)
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        g++ cmake ninja-build pkg-config \
        libprotobuf-dev protobuf-compiler \
        libgrpc++-dev protobuf-compiler-grpc \
        librocksdb-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# Installs raft_server and raft_client into /usr/local/bin.
RUN cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build \
    && cmake --install build --prefix /usr/local \
    && rm -rf /src/build

# RocksDB data (Raft term/vote/log + the KV state machine) lives here.
VOLUME /data
EXPOSE 50051 50052 50053

# `docker run raft-kv --id=1 ...` starts a node; the client is raft_client.
ENTRYPOINT ["raft_server"]
