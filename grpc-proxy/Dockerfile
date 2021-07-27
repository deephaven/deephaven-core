# Deliberately using old alpine, see issue #1034 in deephaven-core
# See also alpinelinux/docker-alpine#155
FROM docker.io/library/alpine:3.12

COPY contents/ .

ENV GRPCWEBPROXY_VERSION=v0.13.0

# Note(deephaven-core#599): Consider moving grpcwebproxy DL into gradle
RUN set -eux; \
    mkdir /app; \
    apk add --no-cache tini; \
    wget -q "https://github.com/improbable-eng/grpc-web/releases/download/${GRPCWEBPROXY_VERSION}/grpcwebproxy-${GRPCWEBPROXY_VERSION}-linux-x86_64.zip"; \
    sha256sum -c checksums.txt; \
    unzip -d /app "grpcwebproxy-${GRPCWEBPROXY_VERSION}-linux-x86_64.zip"; \
    rm "grpcwebproxy-${GRPCWEBPROXY_VERSION}-linux-x86_64.zip"; \
    mv "/app/dist/grpcwebproxy-${GRPCWEBPROXY_VERSION}-linux-x86_64" /app/dist/grpcwebproxy

EXPOSE 8008 8443
USER 1000
ENTRYPOINT ["/sbin/tini", "--", "/docker-entrypoint.sh"]
