FROM deephaven/node:local-build
WORKDIR /usr/src/app

ARG WEB_VERSION=0.16.0

# Pull in the published code-studio package from npmjs and extract is
RUN set -eux; \
    npm pack @deephaven/code-studio@${WEB_VERSION}; \
    tar --touch -xf deephaven-code-studio-${WEB_VERSION}.tgz; \
    mv package/build ide; \
    rm -r package; \
    rm deephaven-code-studio-${WEB_VERSION}.tgz;

# Pull in the published embed-grid package from npmjs and extract is
RUN set -eux; \
    npm pack @deephaven/embed-grid@${WEB_VERSION}; \
    tar --touch -xf deephaven-embed-grid-${WEB_VERSION}.tgz; \
    mkdir -p iframe; \
    mv package/build iframe/table; \
    rm -r package; \
    rm deephaven-embed-grid-${WEB_VERSION}.tgz;

# Pull in the published embed-chart package from npmjs and extract is
RUN set -eux; \
    npm pack @deephaven/embed-chart@${WEB_VERSION}; \
    tar --touch -xf deephaven-embed-chart-${WEB_VERSION}.tgz; \
    mkdir -p iframe; \
    mv package/build iframe/chart; \
    rm -r package; \
    rm deephaven-embed-chart-${WEB_VERSION}.tgz;
