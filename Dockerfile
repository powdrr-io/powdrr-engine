# syntax=docker/dockerfile:1

# Comments are provided throughout this file to help you get started.
# If you need more help, visit the Dockerfile reference guide at
# https://docs.docker.com/engine/reference/builder/

################################################################################
# Create a stage for building the application.

ARG RUST_VERSION=1.89.0
ARG APP_NAME=monolith
FROM rust:${RUST_VERSION}-slim-bullseye AS build
ARG APP_NAME
WORKDIR /app

# RUN rustup target add x86_64-unknown-linux-musl
RUN apt update && apt install -y musl-tools musl-dev libssl-dev pkg-config
RUN update-ca-certificates



# Build the application.
# Leverage a cache mount to /usr/local/cargo/registry/
# for downloaded dependencies and a cache mount to /app/target/ for
# compiled dependencies which will speed up subsequent builds.
# Leverage a bind mount to the src directory to avoid having to copy the
# source code into the container. Once built, copy the executable to an
# output directory before the cache mounted /app/target is unmounted.
#RUN --mount=type=bind,source=service/src,target=app/service/src \
#    --mount=type=bind,source=service/Cargo.toml,target=app/service/Cargo.toml \
#    --mount=type=bind,source=benchmark/src,target=app/benchmark/src \
#    --mount=type=bind,source=benchmark/Cargo.toml,target=app/benchmark/Cargo.toml \
#    --mount=type=bind,source=cli/src,target=cli/src \
#    --mount=type=bind,source=cli/Cargo.toml,target=cli/Cargo.toml \
#    --mount=type=bind,source=engine/src,target=engine/src \
#    --mount=type=bind,source=engine/Cargo.toml,target=engine/Cargo.toml \
#    --mount=type=bind,source=main_lib/src,target=main_lib/src \
#    --mount=type=bind,source=main_lib/Cargo.toml,target=main_lib/Cargo.toml \
#    --mount=type=bind,source=Cargo.toml,target=Cargo.toml \
#    --mount=type=bind,source=Cargo.lock,target=Cargo.lock \
#    --mount=type=cache,target=/app/build/target/ \
#    --mount=type=cache,target=/usr/local/cargo/registry/ \
#    <<EOF
COPY . .
RUN cargo build
