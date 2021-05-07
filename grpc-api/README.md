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
