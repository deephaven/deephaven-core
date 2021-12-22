FROM deephaven/slim-base:local-build as install

COPY licenses/ /

ARG DEEPHAVEN_VERSION

ADD serverApplicationDist/server-${DEEPHAVEN_VERSION}.tar /opt/deephaven

RUN set -eux; \
    ln -s /opt/deephaven/server-${DEEPHAVEN_VERSION} /opt/deephaven/server

COPY image-bootstrap.properties image-bootstrap.properties

FROM deephaven/slim-base:local-build

COPY --from=install / /
VOLUME /data
VOLUME /cache
# Breakdown of the following command:
#   * Wait 3 seconds before checking the status of the container after each test.
#   * After three failing checks, mark the container as failed.
#   * The HEALTHCHECK will wait 11 seconds for a result, but the grpc_health_probe process
#     will time out after only 10 seconds of trying to connect. This one second leaves room
#     for the grpc probe to have a delay starting up, and gives some visiblity into this
#     failure mode (via docker inspect, but only until that failure stales out).
#   * If grpc_health_probe fails with any non-zero exit code, normalize to 1.
# The net effect is that the server will have 3s until the earliest possible success, and
# 33-36s until it is considered to have failed startup (so downstream docker-compose services
# will not attempt to start). If the server is running but trying to shut down, failure will
# occur within about 9s, as the port will connect correctly, but respond that it isn't
# available.
HEALTHCHECK --interval=3s --retries=3 --timeout=11s CMD /bin/grpc_health_probe -addr=localhost:8080 -connect-timeout=10s || exit 1
ENTRYPOINT [ "/opt/deephaven/server/bin/start", "image-bootstrap.properties" ]

ARG DEEPHAVEN_VERSION
LABEL \
    io.deephaven.image.type="server" \
    io.deephaven.server.flavor="server-slim" \
    io.deephaven.version="${DEEPHAVEN_VERSION}" \
    org.opencontainers.image.vendor="Deephaven Data Labs" \
    org.opencontainers.image.title="Deephaven Server - server-slim" \
    org.opencontainers.image.description="Deephaven Community Core Server - server-slim" \
    org.opencontainers.image.licenses="Deephaven Community License Agreement 1.0"