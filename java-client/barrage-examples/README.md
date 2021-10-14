## Deephaven client examples

Deephaven client examples is a collection of example applications built using `client-deephaven`, 
`client-deephaven-dagger`, `client-flight`, `client-flight-dagger`, `client-session`, and `client-session-dagger`.

### Local build

```shell
./gradlew java-client-barrage-examples:installDist
```

produces: 

* `java-client/barrage-examples/build/install/java-client-barrage-examples`.

### Local running

```shell
java-client/barrage-examples/build/install/java-client-barrage-examples/bin/<program> --help
```

### Build

```shell
./gradlew java-client-barrage-examples:build
```

produces:

* `java-client/barrage-examples/build/distributions/java-client-barrage-examples-<version>.zip`
* `java-client/barrage-examples/build/distributions/java-client-barrage-examples-<version>.tar`