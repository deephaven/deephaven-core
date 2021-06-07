FROM deephaven/protoc:local-build

COPY src/main/proto /includes

RUN set -eux; \
  mkdir -p /generated/java; \
  mkdir -p /generated/grpc; \
  mkdir -p /generated/js; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-grpc=/opt/protoc-gen-grpc-java \
    --plugin=protoc-gen-ts=/usr/src/app/node_modules/.bin/protoc-gen-ts \
    --java_out=/generated/java \
    --js_out=import_style=commonjs:/generated/js \
    --grpc_out=/generated/grpc \
    --ts_out=service=grpc-web:/generated/js \
    -I/includes \
    /includes/io/deephaven/proto/barrage.proto \
    /includes/io/deephaven/proto/console.proto \
    /includes/io/deephaven/proto/legacy_comm.proto \
    /includes/io/deephaven/proto/session.proto \
    /includes/io/deephaven/proto/table.proto