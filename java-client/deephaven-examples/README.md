## Deephaven client examples

Deephaven client examples is a collection of example applications built using `client-deephaven`, 
`client-deephaven-dagger`, `client-flight`, `client-flight-dagger`, `client-session`, and `client-session-dagger`.

### Local build

```shell
./gradlew client-deephaven-examples:installDist
```

produces: 

* `java-client/deephaven-examples/build/install/client-deephaven-examples`.

### Local running

```shell
java-client/deephaven-examples/build/install/client-deephaven-examples/bin/<program> --help
```

### Build

```shell
./gradlew client-deephaven-examples:build
```

produces:

* `java-client/deephaven-examples/build/distributions/client-deephaven-examples-<version>.zip`
* `java-client/deephaven-examples/build/distributions/client-deephaven-examples-<version>.tar`