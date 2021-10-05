FROM deephaven/protoc:local-build

COPY src/main/proto /includes

COPY dependencies /dependencies

RUN set -eux; \
  mkdir -p /generated/java; \
  mkdir -p /generated/grpc; \
  mkdir -p /generated/js; \
  mkdir -p /generated/python; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-grpc=/opt/protoc-gen-grpc-java \
    --java_out=/generated/java \
    --grpc_out=/generated/grpc \
    -I/dependencies \
    -I/includes \
    /dependencies/BrowserFlight.proto \
    /includes/deephaven/proto/ticket.proto \
    /includes/deephaven/proto/console.proto \
    /includes/deephaven/proto/session.proto \
    /includes/deephaven/proto/table.proto \
    /includes/deephaven/proto/application.proto \
    /includes/grpc/health/v1/health.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-ts=/usr/src/app/node_modules/.bin/protoc-gen-ts \
    --js_out=import_style=commonjs:/generated/js \
    --ts_out=service=grpc-web:/generated/js \
    -I/dependencies \
    -I/includes \
    /dependencies/BrowserFlight.proto \
    /dependencies/Flight.proto \
    /includes/deephaven/proto/ticket.proto \
    /includes/deephaven/proto/console.proto \
    /includes/deephaven/proto/session.proto \
    /includes/deephaven/proto/table.proto \
    /includes/deephaven/proto/application.proto \
    /includes/grpc/health/v1/health.proto; \
  python3 -m grpc_tools.protoc \
    --grpc_python_out=/generated/python \
    --python_out=/generated/python \
    -I/includes \
    /includes/deephaven/proto/ticket.proto \
    /includes/deephaven/proto/console.proto \
    /includes/deephaven/proto/session.proto \
    /includes/deephaven/proto/table.proto \
    /includes/deephaven/proto/application.proto \
    /includes/grpc/health/v1/health.proto;

