FROM deephaven/node:local-build
WORKDIR /usr/src/app

ARG WEB_EMBED_GRID_VERSION=0.9.1-embed-grid.14

# Pull in the published package from npmjs and extract is
RUN set -eux; \
    npm pack @deephaven/embed-grid@${WEB_EMBED_GRID_VERSION}; \
    tar --touch -xf deephaven-embed-grid-${WEB_EMBED_GRID_VERSION}.tgz; \
    rm deephaven-embed-grid-${WEB_EMBED_GRID_VERSION}.tgz;