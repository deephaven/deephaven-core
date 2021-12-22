# https://www.envoyproxy.io/docs/envoy/latest/start/docker#build-and-run-a-docker-image
FROM deephaven/envoyproxy:local-build
COPY contents/ /
RUN set -eux; \
    mv /envoy.yaml /etc/envoy/envoy.yaml; \
    chmod go+r /etc/envoy/envoy.yaml

ARG DEEPHAVEN_VERSION
LABEL \
    io.deephaven.image.type="envoy" \
    io.deephaven.version="${DEEPHAVEN_VERSION}" \
    org.opencontainers.image.vendor="Deephaven Data Labs" \
    org.opencontainers.image.title="Deephaven Envoy" \
    org.opencontainers.image.description="Deephaven envoy proxy" \
    org.opencontainers.image.licenses="Apache-2.0"