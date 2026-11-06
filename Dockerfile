FROM rust:1.89-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN groupadd --gid 10001 kv && useradd --uid 10001 --gid kv --no-create-home kv \
    && mkdir -p /data && chown kv:kv /data && chmod 700 /data
COPY --from=builder /build/target/release/expense-kv /usr/local/bin/expense-kv
USER kv:kv
WORKDIR /data
ENV KV_DATA_DIR=/data KV_BIND=0.0.0.0:6380
EXPOSE 6380
ENTRYPOINT ["expense-kv"]
