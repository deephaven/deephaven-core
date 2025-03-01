FROM deephaven/protoc-base:local-build

RUN set -eux; \
    apt-get -qq update; \
    apt-get install -qq -y --no-install-recommends openjdk-17-jdk

COPY src/main/proto /includes

COPY dependencies /dependencies

COPY authzPlugin /authzPlugin

RUN chmod +x /authzPlugin/protoc-gen-contextual-auth-wiring
RUN chmod +x /authzPlugin/protoc-gen-service-auth-wiring

RUN set -eux; \
  mkdir -p /generated/java; \
  mkdir -p /generated/grpc; \
  mkdir -p /generated/js; \
  mkdir -p /generated/go; \
  mkdir -p /generated/python; \
  mkdir -p /generated/cpp; \
  mkdir -p /generated/proto-doc/single-html; \
  mkdir -p /generated/proto-doc/single-md; \
  mkdir -p /generated/proto-doc/multi-html; \
  mkdir -p /generated/proto-doc/multi-md; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-grpc=/opt/java/bin/protoc-gen-grpc-java \
    --plugin=protoc-gen-contextual-auth=/authzPlugin/protoc-gen-contextual-auth-wiring \
    --plugin=protoc-gen-service-auth=/authzPlugin/protoc-gen-service-auth-wiring \
    --java_out=/generated/java \
    --grpc_out=/generated/grpc \
    --contextual-auth_out=/generated/java \
    --service-auth_out=/generated/java \
    -I/dependencies \
    -I/includes \
    /dependencies/BrowserFlight.proto \
    /includes/deephaven_core/proto/ticket.proto \
    /includes/deephaven_core/proto/contextual-auth.proto \
    /includes/deephaven_core/proto/console.proto \
    /includes/deephaven_core/proto/object.proto \
    /includes/deephaven_core/proto/session.proto \
    /includes/deephaven_core/proto/table.proto \
    /includes/deephaven_core/proto/application.proto \
    /includes/deephaven_core/proto/inputtable.proto \
    /includes/deephaven_core/proto/partitionedtable.proto \
    /includes/deephaven_core/proto/config.proto \
    /includes/deephaven_core/proto/hierarchicaltable.proto \
    /includes/deephaven_core/proto/storage.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-ts=/usr/src/app/node_modules/.bin/protoc-gen-ts \
    --js_out=import_style=commonjs_strict:/generated/js \
    --ts_out=service=grpc-web:/generated/js \
    -I/dependencies \
    -I/includes \
    /dependencies/BrowserFlight.proto \
    /dependencies/Flight.proto \
    /includes/deephaven_core/proto/ticket.proto \
    /includes/deephaven_core/proto/contextual-auth.proto \
    /includes/deephaven_core/proto/console.proto \
    /includes/deephaven_core/proto/object.proto \
    /includes/deephaven_core/proto/session.proto \
    /includes/deephaven_core/proto/table.proto \
    /includes/deephaven_core/proto/application.proto \
    /includes/deephaven_core/proto/inputtable.proto \
    /includes/deephaven_core/proto/partitionedtable.proto \
    /includes/deephaven_core/proto/config.proto \
    /includes/deephaven_core/proto/hierarchicaltable.proto \
    /includes/deephaven_core/proto/storage.proto; \
  python3 -m grpc_tools.protoc \
    --grpc_python_out=/generated/python \
    --python_out=/generated/python \
    -I/includes \
    /includes/deephaven_core/proto/ticket.proto \
    /includes/deephaven_core/proto/contextual-auth.proto \
    /includes/deephaven_core/proto/console.proto \
    /includes/deephaven_core/proto/object.proto \
    /includes/deephaven_core/proto/session.proto \
    /includes/deephaven_core/proto/table.proto \
    /includes/deephaven_core/proto/application.proto \
    /includes/deephaven_core/proto/inputtable.proto \
    /includes/deephaven_core/proto/partitionedtable.proto \
    /includes/deephaven_core/proto/config.proto \
    /includes/deephaven_core/proto/hierarchicaltable.proto \
    /includes/deephaven_core/proto/storage.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-go=/opt/go/bin/protoc-gen-go \
    --plugin=protoc-gen-go-grpc=/opt/go/bin/protoc-gen-go-grpc \
    --plugin=protoc-gen-cpp_grpc=/opt/deephaven/bin/grpc_cpp_plugin \
    --go_out=/generated/go \
    --go-grpc_out=/generated/go \
    --go_opt=module=github.com/deephaven/deephaven-core/go \
    --go-grpc_opt=module=github.com/deephaven/deephaven-core/go \
    --cpp_out=generated/cpp \
    --cpp_grpc_out=generated/cpp \
    -I/includes \
    /includes/deephaven_core/proto/ticket.proto \
    /includes/deephaven_core/proto/contextual-auth.proto \
    /includes/deephaven_core/proto/console.proto \
    /includes/deephaven_core/proto/object.proto \
    /includes/deephaven_core/proto/session.proto \
    /includes/deephaven_core/proto/table.proto \
    /includes/deephaven_core/proto/application.proto \
    /includes/deephaven_core/proto/inputtable.proto \
    /includes/deephaven_core/proto/partitionedtable.proto \
    /includes/deephaven_core/proto/config.proto \
    /includes/deephaven_core/proto/hierarchicaltable.proto \
    /includes/deephaven_core/proto/storage.proto; \
  # proto-doc-gen does not support writing multiple proto files to multiple docs, must break up command \
  # we are going to generate 4 sets of docs - single html, multi html, single markdown, multi markdown \
  # first, single html \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/single-html \
    --doc_opt=html,index.html \
    -I/includes \
    /includes/deephaven_core/proto/*.proto; \
  # next, single markdown \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/single-md \
    --doc_opt=markdown,index.md \
    -I/includes \
    /includes/deephaven_core/proto/*.proto; \
  # then, multi html \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-html \
    --doc_opt=html,ticket.html \
    -I/includes \
    /includes/deephaven_core/proto/ticket.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-html \
    --doc_opt=html,console.html \
    -I/includes \
    /includes/deephaven_core/proto/console.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-html \
    --doc_opt=html,object.html \
    -I/includes \
    /includes/deephaven_core/proto/object.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-html \
    --doc_opt=html,session.html \
    -I/includes \
    /includes/deephaven_core/proto/session.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-html \
    --doc_opt=html,table.html \
    -I/includes \
    /includes/deephaven_core/proto/table.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-html \
    --doc_opt=html,application.html \
    -I/includes \
    /includes/deephaven_core/proto/application.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-html \
    --doc_opt=html,inputtable.html \
    -I/includes \
    /includes/deephaven_core/proto/inputtable.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-html \
    --doc_opt=html,partitionedtable.html \
    -I/includes \
    /includes/deephaven_core/proto/partitionedtable.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-html \
    --doc_opt=html,config.html \
    -I/includes \
    /includes/deephaven_core/proto/config.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-html \
    --doc_opt=html,hierarchicaltable.html \
    -I/includes \
    /includes/deephaven_core/proto/hierarchicaltable.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-html \
    --doc_opt=html,storage.html \
    -I/includes \
    /includes/deephaven_core/proto/storage.proto; \
  # finally, multi md \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-md \
    --doc_opt=markdown,ticket.md \
    -I/includes \
    /includes/deephaven_core/proto/ticket.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-md \
    --doc_opt=markdown,console.md \
    -I/includes \
    /includes/deephaven_core/proto/console.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-md \
    --doc_opt=markdown,object.md \
    -I/includes \
    /includes/deephaven_core/proto/object.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-md \
    --doc_opt=markdown,session.md \
    -I/includes \
    /includes/deephaven_core/proto/session.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-md \
    --doc_opt=markdown,table.md \
    -I/includes \
    /includes/deephaven_core/proto/table.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-md \
    --doc_opt=markdown,application.md \
    -I/includes \
    /includes/deephaven_core/proto/application.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-md \
    --doc_opt=markdown,inputtable.md \
    -I/includes \
    /includes/deephaven_core/proto/inputtable.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-md \
    --doc_opt=markdown,partitionedtable.md \
    -I/includes \
    /includes/deephaven_core/proto/partitionedtable.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-md \
    --doc_opt=markdown,config.md \
    -I/includes \
    /includes/deephaven_core/proto/config.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-md \
    --doc_opt=markdown,hierarchicaltable.md \
    -I/includes \
    /includes/deephaven_core/proto/hierarchicaltable.proto; \
  /opt/protoc/bin/protoc \
    --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
    --doc_out=generated/proto-doc/multi-md \
    --doc_opt=markdown,storage.md \
    -I/includes \
    /includes/deephaven_core/proto/storage.proto; \
