# This arg must go first otherwise Docker thinks it is part of pypi-version
# if it is above the server FROM line
ARG VERSION=latest
FROM alpine/curl AS pypi-version
WORKDIR /src
# This is a hack to bust the cache when the plugin version changes
# https://github.com/moby/moby/issues/1996#issuecomment-185872769
ARG CACHEBUST=1
RUN curl https://pypi.org/pypi/pydeephaven/json > ./plugin-version.json

FROM ghcr.io/deephaven/server:${VERSION}
# This COPY will cache if the plugin version hasn't changed and skip the pip install
COPY --from=pypi-version /src/plugin-version.json /tmp/plugin-version.json
RUN pip install --no-cache-dir pydeephaven
