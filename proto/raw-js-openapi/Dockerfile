FROM deephaven/proto-backplane-grpc:local-build AS proto-backplane-grpc

FROM docker.io/library/node:14.16.0 AS sources
WORKDIR /usr/src/app
# Note: we are setting CI=true, even for local development, otherwise commands may run in dev-mode (ie,
# `npm run test` watches the filesystem for changes)
ENV CI=true
COPY . .

FROM sources as node_modules
# produces ./node_modules. Need the --unsafe-perm flag as we run as root in the docker container
# https://docs.npmjs.com/cli/v6/using-npm/config#unsafe-perm
RUN set -eux; \
    npm ci --unsafe-perm

FROM node_modules as build

# TODO: this gets TS files which we don't need
COPY --from=proto-backplane-grpc generated/js raw-js-openapi/build/js-src

WORKDIR /usr/src/app/raw-js-openapi
RUN set -eux; \
    ../node_modules/.bin/webpack
