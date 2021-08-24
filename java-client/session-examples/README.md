## Session examples

Session examples is a collection of example applications built using `java-client-session` and `java-client-session-dagger`.

### Local build

```shell
./gradlew java-client-session-examples:installDist
```

produces:

* `java-client/session-examples/build/install/java-client-session-examples`.

### Local running

```shell
java-client/session-examples/build/install/java-client-session-examples/bin/<program> --help
```

### Build

```shell
./gradlew java-client-session-examples:build
```

produces:

* `java-client/session-examples/build/distributions/java-client-session-examples-<version>.zip`
* `java-client/session-examples/build/distributions/java-client-session-examples-<version>.tar`