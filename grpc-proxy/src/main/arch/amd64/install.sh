#!/usr/bin/env sh

set -o errexit
set -o nounset
set -o xtrace

wget -q "https://github.com/improbable-eng/grpc-web/releases/download/${GRPCWEBPROXY_VERSION}/grpcwebproxy-${GRPCWEBPROXY_VERSION}-linux-x86_64.zip"
sha256sum -c /arch/checksums.txt

mkdir /app
unzip -d /app "grpcwebproxy-${GRPCWEBPROXY_VERSION}-linux-x86_64.zip"
rm "grpcwebproxy-${GRPCWEBPROXY_VERSION}-linux-x86_64.zip"
mv "/app/dist/grpcwebproxy-${GRPCWEBPROXY_VERSION}-linux-x86_64" /app/dist/grpcwebproxy

apk add --no-cache tini
