FROM rust:1.77-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /app
COPY Cargo.toml Cargo.lock ./

RUN mkdir src && echo 'fn main(){}' > src/main.rs && \
    cargo build --release && \
    rm src/main.rs

COPY src ./src
RUN touch src/main.rs && cargo build --release


FROM alpine:3.19

RUN apk add --no-cache libcap && \
    adduser -D -u 1000 repeater

COPY --from=builder /app/target/release/mdns-repeater /usr/local/bin/mdns-repeater

RUN setcap cap_net_admin,cap_net_raw+ep /usr/local/bin/mdns-repeater

USER repeater

ENV LOG_FORMAT=pretty
ENV LOG_LEVEL=info
ENV STATS_INTERVAL_SECS=60

ENTRYPOINT ["/usr/local/bin/mdns-repeater"]