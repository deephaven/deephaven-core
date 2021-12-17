FROM deephaven/alpine:local-build as install

# Note(deephaven-core#599): Consider moving grpcwebproxy DL into gradle

ARG TARGETARCH
COPY arch/${TARGETARCH} /arch
RUN set -eux; \
    GRPCWEBPROXY_VERSION=v0.13.0 /arch/install.sh; \
    rm -r /arch

COPY contents/ .

FROM deephaven/alpine:local-build
COPY --from=install / /
EXPOSE 8080 8443
ENTRYPOINT ["/sbin/tini", "--", "/docker-entrypoint.sh"]

ARG DEEPHAVEN_VERSION
LABEL \
    io.deephaven.image.type="grpc-proxy" \
    io.deephaven.version="${DEEPHAVEN_VERSION}" \
    org.opencontainers.image.vendor="Deephaven Data Labs" \
    org.opencontainers.image.title="Deephaven gRPC Web Proxy" \
    org.opencontainers.image.description="Deephaven gRPC web proxy, for the improbable-eng grpc to grpc-web/websocket proxy" \
    org.opencontainers.image.licenses="Apache-2.0"
