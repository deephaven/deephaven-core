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
    --plugin=protoc-gen-ts=/usr/src/app/node_modules/.bin/protoc-gen-ts \
    --java_out=/generated/java \
    --js_out=import_style=commonjs:/generated/js \
    --grpc_out=/generated/grpc \
    --ts_out=service=grpc-web:/generated/js \
    --python_out=/generated/python \
    -I/dependencies \
    -I/includes \
    /dependencies/flight.proto \
    /includes/Barrage.proto \
    /includes/Console.proto \
    /includes/Session.proto \
    /includes/Table.proto; \
  python3 -m grpc_tools.protoc \
    --grpc_python_out=/generated/python \
    -I/dependencies \
    -I/includes \
    /includes/Barrage.proto \
    /includes/Console.proto \
    /includes/Session.proto \
    /includes/Table.proto;

