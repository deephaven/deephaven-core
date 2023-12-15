FROM deephaven/node:local-build
WORKDIR /usr/src/app
# Note: we are setting CI=true, even for local development, otherwise commands may run in dev-mode (ie,
# `npm run test` watches the filesystem for changes)
ENV CI=true
COPY . .

# produces ./node_modules. Need the --unsafe-perm flag as we run as root in the docker container
# https://docs.npmjs.com/cli/v6/using-npm/config#unsafe-perm
RUN set -eux; \
    npm ci --unsafe-perm

WORKDIR /usr/src/app/raw-js-openapi
RUN set -eux; \
    ../node_modules/.bin/webpack
