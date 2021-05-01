# grpc-proxy

grpc-proxy is used to bridge the TLS/SSL gap to make non-production deployments easier.

## Configuration

The environment variable `BACKEND_ADDR` must be set as necessary.
See [Dockerfile](Dockerfile) for more details.

## Build

To build: `./gradlew grpc-proxy:buildDocker`

## Notes

The implementation uses the [grpc-web](https://github.com/improbable-eng/grpc-web)
grpc -> grpc-web+websocket proxy from improbably-eng. It is currently [alpha-quality](https://github.com/improbable-eng/grpc-web#status).
