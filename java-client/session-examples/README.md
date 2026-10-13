## Session examples

Session examples is a collection of example applications built using `java-client-session`. Each is one
readable file; see [java-client/README.md](../README.md) for how they are structured and tested.

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