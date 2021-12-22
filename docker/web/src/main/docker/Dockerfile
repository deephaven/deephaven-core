FROM deephaven/nginx-base:local-build

COPY licenses/ /
COPY nginx/default.conf /etc/nginx/conf.d/
COPY nginx/nginx.conf /etc/nginx/
COPY nginx/99-init-notebooks.sh /docker-entrypoint.d

COPY static/ /usr/share/nginx/html

VOLUME /tmp
ARG DEEPHAVEN_VERSION
LABEL \
    io.deephaven.image.type="web" \
    io.deephaven.version="${DEEPHAVEN_VERSION}" \
    org.opencontainers.image.vendor="Deephaven Data Labs" \
    org.opencontainers.image.title="Deephaven Web" \
    org.opencontainers.image.description="Deephaven web" \
    org.opencontainers.image.licenses="Deephaven Community License Agreement 1.0"