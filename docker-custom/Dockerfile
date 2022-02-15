FROM node:16 as build
WORKDIR /usr/src/packages

COPY install-plugins.sh /usr/src/packages/install-plugins.sh
COPY plugins.txt /usr/src/packages/plugins.txt

RUN set -eux; \
    ./install-plugins.sh

FROM deephaven/web:local-build
COPY --from=build /usr/src/packages/js-plugins/ /usr/share/nginx/html/js-plugins/