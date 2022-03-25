FROM deephaven/nginx-noroot-base:local-build

USER 0

COPY licenses/ /
COPY --chown=nginx nginx/default.conf /etc/nginx/conf.d/
COPY --chown=nginx nginx/nginx.conf /etc/nginx/
COPY nginx/99-init-notebooks.sh /docker-entrypoint.d
COPY --chown=nginx static/ /usr/share/nginx/html

USER 101

VOLUME /tmp
ARG DEEPHAVEN_VERSION
LABEL \
    io.deephaven.image.type="web" \
    io.deephaven.version="${DEEPHAVEN_VERSION}" \
    org.opencontainers.image.vendor="Deephaven Data Labs" \
    org.opencontainers.image.title="Deephaven Web" \
    org.opencontainers.image.description="Deephaven web" \
    org.opencontainers.image.licenses="Deephaven Community License Agreement 1.0"