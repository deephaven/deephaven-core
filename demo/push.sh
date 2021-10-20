dir="$(pwd)"

while [ -n "$dir" ]; do
  if [ -f "$dir/gradlew" ]; then
    # workaround stale images...
    docker rmi deephaven/grpc-api:local-build &>/dev/null
    docker rmi deephaven/web:local-build &>/dev/null
    docker rmi deephaven/demo-grpc-api:local-build &>/dev/null
    docker rmi deephaven/demo-web:local-build &>/dev/null
    docker rmi us-central1-docker.pkg.dev/deephaven-oss/deephaven/grpc-api:0.5.27 &>/dev/null
    docker rmi us-central1-docker.pkg.dev/deephaven-oss/deephaven/web:0.5.27 &>/dev/null

    "$dir/gradlew" quarkusBuild pushAll -i "$@"
    exit 0
  fi
  dir="$(dirname "$dir")"
done

