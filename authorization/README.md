Deephaven's Approach to Authorization
======================================

Configuration
-------------

To configure authorization, please subclass `io.deephaven.server.auth.AuthorizationProvider` and provide implementations
for the authorization hooks. There are helpful `AllowAll` and `DenyAll` implementations that can be used as a starting
places for your own implementations.

Once you have a subclass of `AuthorizationProvider`, create your own main method following the patterns found in
`io.deephaven.server.JettyMain` and `io.deephaven.server.NettyMain`.

RPC Hook-Based Code Generation
------------------------------

At this time the auth wiring is generated manually via protoc plugins. See issue deephaven-core#3133 for work to
automate this. Here we invoke protoc and delegate to two plugins, one for those that interact with tables and one for
those that do not. The contextual authorization wiring includes a `List<Table>` argument allowing authorization hooks
to be dependent on the tables being operated on.

The two plugins are:
- protoc-gen-auth-wiring which launches `io.deephaven.auth.codegen.GenerateContextualAuthWiring`
- protoc-gen-contextual-auth-wiring  which launches `io.deephaven.auth.codegen.GenerateServiceAuthWiring`

Here is a sample bash script to generate the provided authorizing wiring if you have protoc installed:
```bash
./gradlew :authorization-codegen:shadowJar

DEEPHAVEN_VERSION="$(./gradlew printVersion -q)"
OUT_DIR=authorization/src/main/java/
PROTO_DIR=proto/proto-backplane-grpc/src/main/proto/
ROOT_DIR=$PROTO_DIR/deephaven/proto

DEEPHAVEN_VERSION=${DEEPHAVEN_VERSION} PATH=authorization-codegen:$PATH protoc --service-auth-wiring_out=$OUT_DIR -I $PROTO_DIR    \
     $ROOT_DIR/application.proto                                        \
     $ROOT_DIR/console.proto                                            \
     $ROOT_DIR/config.proto                                             \
     $ROOT_DIR/object.proto                                             \
     $ROOT_DIR/partitionedtable.proto                                   \
     $ROOT_DIR/session.proto                                            \
     $ROOT_DIR/storage.proto                                            \
     $ROOT_DIR/ticket.proto

DEEPHAVEN_VERSION=${DEEPHAVEN_VERSION} PATH=authorization-codegen:$PATH protoc --contextual-auth-wiring_out=$OUT_DIR -I $PROTO_DIR \
     $ROOT_DIR/table.proto                                              \
     $ROOT_DIR/inputtable.proto                                         \
     $ROOT_DIR/partitionedtable.proto                                   \
     $ROOT_DIR/hierarchicaltable.proto

OUT_DIR=authorization/src/main/java/
PROTO_DIR=../grpc/src/proto/grpc/health/v1/
ROOT_DIR=$PROTO_DIR

DEEPHAVEN_VERSION=${DEEPHAVEN_VERSION}  PATH=authorization-codegen:$PATH protoc --service-auth-wiring_out=$OUT_DIR -I $PROTO_DIR    \
     $ROOT_DIR/health.proto

./gradlew :authorization:spotlessApply
```

For protoc versions 3.12.x - 3.14.x, the `--experimental_allow_proto3_optional` flag is needed.
