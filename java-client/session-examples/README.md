## Session examples

Session examples is a collection of example applications built using `client-session` and `client-session-dagger`.

### Local build

```shell
./gradlew client-session-examples:installDist
```

produces:

* `java-client/session-examples/build/install/client-session-examples`.

### Local running

```shell
java-client/session-examples/build/install/client-session-examples/bin/<program> --help
```

### Build

```shell
./gradlew client-session-examples:build
```

produces:

* `java-client/session-examples/build/distributions/client-session-examples-<version>.zip`
* `java-client/session-examples/build/distributions/client-session-examples-<version>.tar`