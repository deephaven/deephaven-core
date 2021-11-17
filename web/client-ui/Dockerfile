FROM deephaven/node:local-build AS node
WORKDIR /usr/src/app

FROM node as build
ARG WEB_VERSION=0.6.2

# Pull in the published package from npmjs and extract is
RUN set -eux; \
    npm pack @deephaven/code-studio@${WEB_VERSION}; \
    tar --touch -xf deephaven-code-studio-${WEB_VERSION}.tgz; \
    rm deephaven-code-studio-${WEB_VERSION}.tgz;