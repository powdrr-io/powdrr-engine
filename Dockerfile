# syntax=docker/dockerfile:1

# Comments are provided throughout this file to help you get started.
# If you need more help, visit the Dockerfile reference guide at
# https://docs.docker.com/engine/reference/builder/

################################################################################
# Create a stage for building the application.

ARG RUST_VERSION=1.89.0
FROM rust:${RUST_VERSION}-slim-bullseye AS build

# RUN rustup target add x86_64-unknown-linux-musl
RUN apt update && apt install -y libssl-dev pkg-config
RUN update-ca-certificates

#COPY Cargo.toml Cargo.lock /app/
#
#RUN cargo new /app/benchmark
#COPY benchmark/Cargo.toml /app/benchmark/
#RUN cargo new /app/cli
#COPY cli/Cargo.toml /app/cli/
#RUN cargo new /app/engine
#COPY engine/Cargo.toml /app/engine/
#RUN cargo new /app/service
#COPY service/Cargo.toml /app/service/
#
#RUN cargo new --lib /app/main_lib
#COPY main_lib/Cargo.toml /app/main_lib/

WORKDIR /app
#
#RUN --mount=type=cache,target=/usr/local/cargo/registry \
#    --mount=type=cache,target=/usr/local/cargo/git \
#    cargo build --release

COPY . .

ARG TARGET

RUN cargo build --release && \
    mv /app/target/release/${TARGET} /app


FROM rust:${RUST_VERSION}-slim-bullseye
ARG PORT
ARG TARGET
COPY --from=build /app/${TARGET} ./
ENV TARGET_ENV=$TARGET
CMD [ "sh", "-c", "./$TARGET_ENV" ]

EXPOSE ${PORT}




