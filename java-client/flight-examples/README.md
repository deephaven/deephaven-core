## Flight examples

Flight examples is a collection of example applications built using `client-flight`, `client-flight-dagger`,
`client-session`, and `client-session-dagger`.

### Local build

```shell
./gradlew java-client-flight-examples:installDist
```

produces: 

* `java-client/flight-examples/build/install/java-client-flight-examples`.

### Local running

```shell
java-client/flight-examples/build/install/java-client-flight-examples/bin/<program> --help
```

### Build

```shell
./gradlew java-client-flight-examples:build
```

produces:

* `java-client/flight-examples/build/distributions/java-client-flight-examples-<version>.zip`
* `java-client/flight-examples/build/distributions/java-client-flight-examples-<version>.tar`