# grpc-proxy

grpc-proxy is a proxy that exposes a gRPC service via websockets, allowing for the gRPC service to
be consumed from browsers.

## Configuration

The environment variable `BACKEND_ADDR` must be set to the gRPC service to be proxied.
See [Dockerfile](Dockerfile) for more details.

## Build

To build: `./gradlew grpc-proxy:buildDocker`

## Notes

The implementation uses the [grpc-web](https://github.com/improbable-eng/grpc-web)
grpc -> grpc-web+websocket proxy from improbably-eng. It is currently [alpha-quality](https://github.com/improbable-eng/grpc-web#status).
