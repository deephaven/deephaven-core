ARG BASE
FROM $BASE as install

COPY licenses/ /

# Note: all of the pip installs have the --no-index flag, with the expectation that all external dependencies are
# already satisfied via the base image. If that is not the case, we want the install to error out, and we'll need to
# update the base image with the extra requirements before proceeding here.

ARG SERVER
COPY $SERVER/requirements.txt requirements.txt
RUN set -eux; \
    python -m pip install -q --no-index --no-cache-dir -r requirements.txt; \
    rm requirements.txt

COPY wheels/ /wheels
RUN set -eux; \
    python -m pip install -q --no-index --no-cache-dir /wheels/*.whl; \
    rm -r /wheels

ARG DEEPHAVEN_VERSION
ADD serverApplicationDist/server-jetty-${DEEPHAVEN_VERSION}.tar /opt/deephaven

RUN set -eux; \
    ln -s /opt/deephaven/server-jetty-${DEEPHAVEN_VERSION} /opt/deephaven/server

FROM $BASE
COPY --from=install / /
VOLUME /data
VOLUME /cache
HEALTHCHECK --interval=3s --retries=3 --timeout=11s CMD /opt/grpc_health_probe/grpc_health_probe -addr=localhost:10000 -connect-timeout=10s || exit 1
ENV \
    DEEPHAVEN_CACHE_DIR="/cache" \
    DEEPHAVEN_CONFIG_DIR="/opt/deephaven/config" \
    DEEPHAVEN_DATA_DIR="/data" \
    EXTRA_CLASSPATH="/apps/libs/*"
ENTRYPOINT [ "/opt/deephaven/server/bin/start" ]
ARG DEEPHAVEN_VERSION
ARG SERVER
LABEL \
    io.deephaven.image.type="server" \
    io.deephaven.server.flavor="${SERVER}" \
    io.deephaven.version="${DEEPHAVEN_VERSION}" \
    org.opencontainers.image.vendor="Deephaven Data Labs" \
    org.opencontainers.image.title="Deephaven Server - ${SERVER}" \
    org.opencontainers.image.description="Deephaven Community Core Server - ${SERVER}" \
    org.opencontainers.image.licenses="Deephaven Community License Agreement 1.0"