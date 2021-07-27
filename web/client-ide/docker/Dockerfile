FROM deephaven/dhide:local-build as dhide

FROM deephaven/js-out:local-build as js-out

FROM nginxinc/nginx-unprivileged:1.19.7-amd64

USER 0

# these installs are necessary to enable webdav class 2
# https://github.com/arut/nginx-dav-ext-module
RUN set -eux; \
    apt-get update; \
    apt-get install -y nginx-extras libnginx-mod-http-dav-ext --option=Dpkg::Options::=--force-confdef;

RUN mkdir -p /data/notebooks; \
    chown nginx:nginx /data/notebooks; \
    chown -R nginx:nginx /var/lib/nginx;

# nginx user gets uid 101
USER 101

COPY --from=dhide /usr/src/app/package/build /usr/share/nginx/html/ide
COPY --from=js-out /usr/src/app/raw-js-openapi/build/js-out /usr/share/nginx/html/jsapi
COPY licenses/ /
COPY dhapi /usr/share/nginx/html/jsapi
COPY default.conf /etc/nginx/conf.d/
COPY nginx.conf /etc/nginx/
COPY 99-init-notebooks.sh /docker-entrypoint.d

VOLUME /tmp