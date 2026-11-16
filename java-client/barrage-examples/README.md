## Deephaven barrage examples

Deephaven barrage examples is a collection of example applications built using `java-client-barrage`. Each is
one readable file; see [java-client/README.md](../README.md) for how they are structured and tested.

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
