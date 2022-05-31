# TODO(deephaven-base-images#13): Create base image for web-plugin-packager
FROM node:16-buster-slim
COPY files/ .

ARG DEEPHAVEN_VERSION
LABEL \
    io.deephaven.image.type="web-plugin-packager" \
    io.deephaven.version="${DEEPHAVEN_VERSION}" \
    org.opencontainers.image.vendor="Deephaven Data Labs" \
    org.opencontainers.image.title="Deephaven Web Plugin Packager" \
    org.opencontainers.image.description="Deephaven Web Plugin Packager" \
    org.opencontainers.image.licenses="Deephaven Community License Agreement 1.0"
