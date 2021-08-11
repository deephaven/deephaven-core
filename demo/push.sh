dir="$(pwd)"
while [ -n "$dir" ]; do
  if [ -f "$dir/gradlew" ]; then
    "$dir/gradlew" quarkusBuild -Dquarkus.container-image.build=true -Dquarkus.container-image.push=true -Dquarkus.container-image.builder=docker -i
    exit 0
  fi
  dir="$(dirname "$dir")"
done

