FROM deephaven/node:local-build
WORKDIR /usr/src/app

ARG WEB_VERSION=0.7.2

# Pull in the published package from npmjs and extract is
RUN set -eux; \
    npm pack @deephaven/code-studio@${WEB_VERSION}; \
    tar --touch -xf deephaven-code-studio-${WEB_VERSION}.tgz; \
    rm deephaven-code-studio-${WEB_VERSION}.tgz;