# Basing our image on node, so we can easily install and invoke ts-protoc-gen protoc plugin
FROM deephaven/node:local-build

WORKDIR /usr/src/app

# protoc includes support for python, but not support for grpc python, we need pip to get it
RUN set -eux; \
    apt-get -qq update; \
    apt-get -qq -y --no-install-recommends install python3-pip wget unzip; \
    rm -rf /var/lib/apt/lists/*; \
    python3 -m pip install --no-cache-dir --upgrade pip; \
    python3 -m pip install --no-cache-dir grpcio-tools

COPY checksums.txt .

# This version should be the same as the version found in package.json/package-lock.json
RUN set -eux; \
    wget -q "https://github.com/protocolbuffers/protobuf/releases/download/v3.15.6/protoc-3.15.6-linux-x86_64.zip"; \
    wget -q "https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/1.36.1/protoc-gen-grpc-java-1.36.1-linux-x86_64.exe"; \
    sha256sum --check checksums.txt; \
    unzip protoc-3.15.6-linux-x86_64.zip -d /opt/protoc; \
    mv protoc-gen-grpc-java-1.36.1-linux-x86_64.exe /opt/protoc-gen-grpc-java; \
    chmod +x /opt/protoc-gen-grpc-java; \
    rm protoc-3.15.6-linux-x86_64.zip

COPY . .

RUN set -eux; \
    npm ci

#ENTRYPOINT ["/opt/protoc/bin/protoc", \
#  "--plugin=\"protoc-gen-ts=./node_modules/.bin/protoc-gen-ts\"", \
#  "--js_out=\"import_style=commonjs,binary:./generated\"", \
#  "--ts_out=\"./generated\"" ]