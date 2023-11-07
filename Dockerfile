FROM ghcr.io/cross-rs/arm-unknown-linux-gnueabihf:main
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt install -qy protobuf-compiler