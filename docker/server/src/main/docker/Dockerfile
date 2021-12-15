ARG BASE
FROM $BASE as install

COPY licenses/ /

# Note: all of the pip installs have the --no-index flag, with the expectation that all external dependencies are
# already satisfied via the base image. If that is not the case, we want the install to error out, and we'll need to
# update the base image with the extra requirements before proceeding here.

ARG SERVER
COPY $SERVER/requirements.txt requirements.txt
RUN set -eux; \
    python3 -m pip install -q --no-index --no-cache-dir -r requirements.txt; \
    rm requirements.txt

COPY deephaven-jpy-wheel/ /deephaven-jpy-wheel
RUN set -eux; \
    python3 -m pip install -q --no-index --no-cache-dir /deephaven-jpy-wheel/*.whl; \
    rm -r /deephaven-jpy-wheel

COPY deephaven-wheel/ /deephaven-wheel
RUN set -eux; \
    python3 -m pip install -q --no-index --no-cache-dir /deephaven-wheel/*.whl; \
    rm -r /deephaven-wheel

COPY deephaven2-wheel/ /deephaven2-wheel
RUN set -eux; \
    python3 -m pip install -q --no-index --no-cache-dir /deephaven2-wheel/*.whl; \
    rm -r /deephaven2-wheel

ARG DEEPHAVEN_VERSION
ADD serverApplicationDist/server-${DEEPHAVEN_VERSION}.tar /opt/deephaven

RUN set -eux; \
    ln -s /opt/deephaven/server-${DEEPHAVEN_VERSION} /opt/deephaven/server

COPY configure/ /configure
RUN set -eux; \
    cp /configure/image-bootstrap.properties image-bootstrap.properties; \
    python3 /configure/properties.py >> image-bootstrap.properties; \
    cat image-bootstrap.properties; \
    rm -r /configure

FROM $BASE
COPY --from=install / /
VOLUME /data
VOLUME /cache
HEALTHCHECK --interval=3s --retries=3 --timeout=11s CMD /bin/grpc_health_probe -addr=localhost:8080 -connect-timeout=10s || exit 1
ENTRYPOINT [ "/opt/deephaven/server/bin/start", "image-bootstrap.properties" ]