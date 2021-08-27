## Flight examples

Flight examples is a collection of example applications built using `client-flight`, `client-flight-dagger`,
`client-session`, and `client-session-dagger`.

### Local build

```shell
./gradlew client-flight-examples:installDist
```

produces: 

* `java-client/flight-examples/build/install/client-flight-examples`.

### Local running

```shell
java-client/flight-examples/build/install/client-flight-examples/bin/<program> --help
```

### Build

```shell
./gradlew client-flight-examples:build
```

produces:

* `java-client/flight-examples/build/distributions/client-flight-examples-<version>.zip`
* `java-client/flight-examples/build/distributions/client-flight-examples-<version>.tar`