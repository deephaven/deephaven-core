### Building wheel with docker

```shell
./gradlew Integrations:buildDockerForWheel
```

or manually,

```shell
docker build --build-arg DEEPHAVEN_VERSION=x.y.z --tag deephaven/deephaven-legacy-wheel --target build Integrations/python
```