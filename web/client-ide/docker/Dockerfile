FROM deephaven/dhide:local-build as dhide

FROM deephaven/js-out:local-build as js-out

FROM docker.io/library/nginx:1.19.7

# these installs are necessary to enable webdav class 2
# https://github.com/arut/nginx-dav-ext-module
RUN set -eux; \
    apt-get update; \
    apt-get install -y nginx-extras libnginx-mod-http-dav-ext;

COPY --from=dhide /usr/src/app/package/build /usr/share/nginx/html/ide
COPY --from=js-out /usr/src/app/raw-js-openapi/build/js-out /usr/share/nginx/html/jsapi
COPY licenses/ /
COPY dhapi /usr/share/nginx/html/jsapi
COPY default.conf /etc/nginx/conf.d/
COPY nginx.conf /etc/nginx/
COPY 99-init-notebooks.sh /docker-entrypoint.d

VOLUME /tmp