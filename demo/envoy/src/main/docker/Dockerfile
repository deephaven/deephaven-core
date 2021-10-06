# https://www.envoyproxy.io/docs/envoy/latest/start/docker#build-and-run-a-docker-image
FROM docker.io/envoyproxy/envoy:v1.17.1
COPY envoy.yaml /etc/envoy/envoy.yaml
RUN set -eux; \
    chmod go+r /etc/envoy/envoy.yaml
