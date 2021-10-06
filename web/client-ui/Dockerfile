FROM docker.io/library/node:14.16.0 AS node
WORKDIR /usr/src/app

FROM node as build
ENV WEB_VERSION=0.4.0

# Pull in the published package from npmjs and extract it
RUN set -eux; \
    npm pack @deephaven/code-studio@${WEB_VERSION}; \
    tar --touch -xf deephaven-code-studio-${WEB_VERSION}.tgz; \
    rm deephaven-code-studio-${WEB_VERSION}.tgz;