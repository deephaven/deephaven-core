Getting Started
===============

Run `io.deephaven.grpc_api.runner.Main` to bring up a gRPC server localhost.
See [grpc-api-native](app/native/README.md) or [grpc-api-docker](app/docker/README.md) for
pre-packaged and configured invocations.

Extending gRPC API
------------------

We use dagger2 for dependency injection.
To inject your own gRPC services, create your own main and inject your own `BindableService`s,
in addition to including `DeephavenApiServerModule`.


### Resolve Tools

[Resolve Tools](src/main/java/io/deephaven/uri/ResolveTools.java) presents top-level static methods for resolving URIs.

Internally, URIs are resolved via a [UriResolvers](src/main/java/io/deephaven/grpc_api/uri/UriResolvers.java),
which may be composed of multiple [UriResolver](src/main/java/io/deephaven/grpc_api/uri/UriResolver.java) instances.

Currently, the following resolvers are installed: 

* [Application Resolver](src/main/java/io/deephaven/grpc_api/uri/ApplicationResolver.java)
* [Barrage Table Resolver](src/main/java/io/deephaven/grpc_api/uri/BarrageTableResolver.java)
* [CSV Table Resolver](src/main/java/io/deephaven/grpc_api/uri/CsvTableResolver.java)
* [Parquet Table Resolver](src/main/java/io/deephaven/grpc_api/uri/ParquetTableResolver.java)
* [Query Scope Resolver](src/main/java/io/deephaven/grpc_api/uri/QueryScopeResolver.java)

See [UriModule](src/main/java/io/deephaven/grpc_api/uri/UriModule.java).

Additional options for configuring resolve rules will be added in the future.