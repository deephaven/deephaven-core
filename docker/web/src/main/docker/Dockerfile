FROM deephaven/nginx-noroot-base:local-build

USER 0

RUN printf '#!/bin/sh\nexit 0' > /usr/sbin/policy-rc.d && \
    apt update -y && \
    apt install sudo -y && \
    rm -rf /var/lib/apt/lists/*

COPY licenses/ /
COPY --chown=nginx nginx/default.conf /etc/nginx/conf.d/
COPY --chown=nginx nginx/nginx.conf /etc/nginx/
COPY nginx/99-init-notebooks.sh /docker-entrypoint.d
COPY --chown=nginx static/ /usr/share/nginx/html

RUN    printf ' \n\
nginx    ALL=(ALL)    NOPASSWD: /bin/mkdir -p /data/notebooks \n\
nginx    ALL=(ALL)    NOPASSWD: /bin/chown nginx.nginx /data/notebooks \n\
nginx    ALL=(ALL)    NOPASSWD: /bin/mkdir -p /data/layouts \n\
nginx    ALL=(ALL)    NOPASSWD: /bin/chown nginx.nginx /data/layouts \n' > /etc/sudoers.d/deephaven;

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