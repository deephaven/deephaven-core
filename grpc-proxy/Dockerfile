FROM docker.io/library/alpine:3.13

COPY contents/ .

ENV GRPCWEBPROXY_VERSION=v0.13.0

RUN set -eux; \
    apk add --no-cache tini; \
    wget -q "https://github.com/improbable-eng/grpc-web/releases/download/${GRPCWEBPROXY_VERSION}/grpcwebproxy-${GRPCWEBPROXY_VERSION}-linux-x86_64.zip"; \
    sha256sum -c checksums.txt; \
    unzip -d /app "grpcwebproxy-${GRPCWEBPROXY_VERSION}-linux-x86_64.zip"; \
    rm "grpcwebproxy-${GRPCWEBPROXY_VERSION}-linux-x86_64.zip"; \
    mv "/app/dist/grpcwebproxy-${GRPCWEBPROXY_VERSION}-linux-x86_64" /app/dist/grpcwebproxy

EXPOSE 8080 8443

ENTRYPOINT ["/sbin/tini", "--", "/docker-entrypoint.sh"]

LABEL org.opencontainers.image.vendor="Deephaven Data Labs" \
  org.opencontainers.image.title="Deephaven gRPC web proxy" \
  org.opencontainers.image.description="Deephaven gRPC web proxy, for the improbable-eng grpc to grpc-web/websocket proxy" \
  org.opencontainers.image.source="https://github.com/deephaven/deephaven-core" \
  org.opencontainers.image.licenses="Apache-2.0"
