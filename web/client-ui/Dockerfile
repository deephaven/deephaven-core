FROM deephaven/node:local-build
WORKDIR /usr/src/app

# Most of the time, these versions are the same, except in cases where a patch only affects one of the packages
ARG WEB_VERSION=0.102.1
ARG GRID_VERSION=0.102.0
ARG CHART_VERSION=0.102.0
ARG WIDGET_VERSION=0.102.1

# Pull in the published code-studio package from npmjs and extract is
RUN set -eux; \
    npm pack @deephaven/code-studio@${WEB_VERSION}; \
    tar --touch -xf deephaven-code-studio-${WEB_VERSION}.tgz; \
    mv package/build ide; \
    rm -r package; \
    rm deephaven-code-studio-${WEB_VERSION}.tgz;

# Pull in the published embed-grid package from npmjs and extract is
RUN set -eux; \
    npm pack @deephaven/embed-grid@${GRID_VERSION}; \
    tar --touch -xf deephaven-embed-grid-${GRID_VERSION}.tgz; \
    mkdir -p iframe; \
    mv package/build iframe/table; \
    rm -r package; \
    rm deephaven-embed-grid-${GRID_VERSION}.tgz;

# Pull in the published embed-chart package from npmjs and extract is
RUN set -eux; \
    npm pack @deephaven/embed-chart@${CHART_VERSION}; \
    tar --touch -xf deephaven-embed-chart-${CHART_VERSION}.tgz; \
    mkdir -p iframe; \
    mv package/build iframe/chart; \
    rm -r package; \
    rm deephaven-embed-chart-${CHART_VERSION}.tgz;

# Pull in the published embed-widget package from npmjs and extract is
RUN set -eux; \
    npm pack @deephaven/embed-widget@${WIDGET_VERSION}; \
    tar --touch -xf deephaven-embed-widget-${WIDGET_VERSION}.tgz; \
    mkdir -p iframe; \
    mv package/build iframe/widget; \
    rm -r package; \
    rm deephaven-embed-widget-${WIDGET_VERSION}.tgz;
