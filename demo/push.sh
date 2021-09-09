dir="$(pwd)"
while [ -n "$dir" ]; do
  if [ -f "$dir/gradlew" ]; then
    "$dir/gradlew" quarkusBuild -Dquarkus.container-image.build=true -Dquarkus.container-image.push=true -Dquarkus.container-image.builder=docker -i
    # Now, we should also run helm upgrade dh-demo "$dir/helm"
    # ...but without nuking the number of running replicas
    exit 0
  fi
  dir="$(dirname "$dir")"
done

