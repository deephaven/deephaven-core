#!/usr/bin/env sh

set -o errexit
set -o nounset
set -o xtrace

wget -q "https://github.com/improbable-eng/grpc-web/releases/download/${GRPCWEBPROXY_VERSION}/grpcwebproxy-${GRPCWEBPROXY_VERSION}-arm64.zip"
sha256sum -c /arch/checksums.txt

mkdir /app /app/dist
unzip -d /app/dist "grpcwebproxy-${GRPCWEBPROXY_VERSION}-arm64.zip"
rm "grpcwebproxy-${GRPCWEBPROXY_VERSION}-arm64.zip"
mv "/app/dist/grpcwebproxy-${GRPCWEBPROXY_VERSION}-arm64" /app/dist/grpcwebproxy

apk add --no-cache tini
