# Should be run with --no-cache to ensure the latest pydeephaven is installed
# This arg must go first otherwise Docker thinks it is part of pypi-version
# if it is above the server FROM line
ARG DEEPHAVEN_SERVER_IMAGE=ghcr.io/deephaven/server:latest
FROM alpine/curl AS pypi-version
WORKDIR /src
RUN curl https://pypi.org/pypi/pydeephaven/json > ./plugin-version.json

FROM ${DEEPHAVEN_SERVER_IMAGE} AS server
# This COPY will cache if the plugin version hasn't changed and skip the pip install
COPY --from=pypi-version /src/plugin-version.json /tmp/plugin-version.json
RUN pip install --no-cache-dir pydeephaven
