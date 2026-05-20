# syntax=docker/dockerfile:1

# Comments are provided throughout this file to help you get started.
# If you need more help, visit the Dockerfile reference guide at
# https://docs.docker.com/engine/reference/builder/

################################################################################
# Create a stage for building the application.

ARG RUST_VERSION=1.92.0
FROM rust:${RUST_VERSION}-slim-bullseye AS base

RUN cargo install cargo-chef

# RUN rustup target add x86_64-unknown-linux-musl
RUN apt update && apt install -y libssl-dev pkg-config
RUN update-ca-certificates

FROM base AS planner
WORKDIR app
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM base AS builder
WORKDIR app
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release

FROM base
ARG PORT
ARG TARGET
WORKDIR app
COPY --from=builder /app/target/release/${TARGET} ./

ENV TARGET_ENV=$TARGET
CMD [ "sh", "-c", "./$TARGET_ENV" ]

EXPOSE ${PORT}



