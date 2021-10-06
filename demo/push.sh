dir="$(pwd)"

while [ -n "$dir" ]; do
  if [ -f "$dir/gradlew" ]; then
    # workaround stale images...
    docker rmi deephaven/grpc-api:local-build &>/dev/null
    docker rmi deephaven/web:local-build &>/dev/null
    docker rmi deephaven/demo-grpc-api:local-build &>/dev/null
    docker rmi deephaven/demo-web:local-build &>/dev/null
    docker rmi us-central1-docker.pkg.dev/deephaven-oss/deephaven/grpc-api:0.5.0 &>/dev/null
    docker rmi us-central1-docker.pkg.dev/deephaven-oss/deephaven/web:0.5.0 &>/dev/null

    export WEB_VERSION=${WEB_VERSION:-0.1.4-markdownnotebooks.103}
    "$dir/gradlew" quarkusBuild pushAll \
        -i \
        -PdockerPath=deephaven-oss/deephaven \
        -PwebVersion=$WEB_VERSION \
        -PextraHealthArgs="-tls -tls-ca-cert=/etc/ssl/internal/ca.crt -tls-client-key=/etc/ssl/internal/tls.key -tls-client-cert=/etc/ssl/internal/tls.crt" \
        -Dquarkus.container-image.build=true \
        -Dquarkus.container-image.push=true \
        -Dquarkus.container-image.builder=docker "$@"
    exit 0
  fi
  dir="$(dirname "$dir")"
done

