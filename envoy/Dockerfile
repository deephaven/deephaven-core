# https://www.envoyproxy.io/docs/envoy/latest/start/docker#build-and-run-a-docker-image
FROM docker.io/envoyproxy/envoy:v1.17.1
COPY contents/ /
RUN set -eux; \
    mv /envoy.yaml /etc/envoy/envoy.yaml; \
    chmod go+r /etc/envoy/envoy.yaml

LABEL org.opencontainers.image.vendor="Deephaven Data Labs" \
  org.opencontainers.image.title="Deephaven envoy" \
  org.opencontainers.image.description="Deephaven envoy proxy" \
  org.opencontainers.image.source="https://github.com/deephaven/deephaven-core" \
  org.opencontainers.image.licenses="Apache-2.0"