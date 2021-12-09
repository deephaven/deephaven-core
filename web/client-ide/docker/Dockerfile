FROM deephaven/dhide:local-build as dhide

FROM deephaven/js-out:local-build as js-out

FROM deephaven/nginx-base:local-build

COPY --from=dhide /usr/src/app/package/build /usr/share/nginx/html/ide
COPY --from=js-out /usr/src/app/raw-js-openapi/build/js-out /usr/share/nginx/html/jsapi
COPY licenses/ /
COPY dhapi /usr/share/nginx/html/jsapi
COPY default.conf /etc/nginx/conf.d/
COPY nginx.conf /etc/nginx/
COPY 99-init-notebooks.sh /docker-entrypoint.d

VOLUME /tmp