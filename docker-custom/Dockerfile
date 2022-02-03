FROM node:14 as build
RUN set -eux; \
    npm pack @deephaven/matplotlib-js-plugin@0.9.5-alpha.1; \
    tar --touch -xf deephaven-matplotlib-js-plugin-0.9.5-alpha.1.tgz

FROM deephaven/web:local-build
COPY --from=build /package /usr/share/nginx/html/js-plugins/@deephaven/matplotlib-js-plugin
COPY manifest.json /usr/share/nginx/html/js-plugins