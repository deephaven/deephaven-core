FROM deephaven/web:local-build

USER 0
RUN mkdir -p /data/notebooks; \
    chown nginx:nginx /data/notebooks; \
    mkdir -p /data/layouts; \
    chown nginx:nginx /data/layouts; \
    chown -R nginx:nginx /var/lib/nginx;

COPY --chown=nginx layouts/ /data/layouts

COPY --chown=nginx notebooks/ /data/notebooks

USER 101
