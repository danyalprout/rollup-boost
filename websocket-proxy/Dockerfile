FROM rust:1.84 AS builder

WORKDIR /app

ARG WEBSOCKET_PROXY_BIN="websocket-proxy"
ARG FEATURES

COPY . .

RUN cargo build --release --features="$FEATURES" --package=${WEBSOCKET_PROXY_BIN}

#
# Runtime container
#
FROM gcr.io/distroless/cc-debian12
WORKDIR /app

ARG WEBSOCKET_PROXY_BIN="websocket-proxy"
COPY --from=builder /app/target/release/${WEBSOCKET_PROXY_BIN} /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/websocket-proxy"]