FROM deephaven/dhide as dhide

FROM deephaven/js-out as js-out

FROM docker.io/library/nginx:1.19.7
COPY --from=dhide /usr/src/app/package/build /usr/share/nginx/html/ide
COPY --from=js-out /usr/src/app/raw-js-openapi/build/js-out /usr/share/nginx/html/jsapi
COPY dhapi /usr/share/nginx/html/jsapi
COPY default.conf /etc/nginx/conf.d
