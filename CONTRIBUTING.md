# Contributing

This guide will serve as a reference for contributing to the Deephaven.

We need more meat to this meal

## Styleguide
The [styleguide](style/README.md) is not global yet.
To opt-in, module build files apply the following:

```groovy
spotless {
  java {
    eclipse().configFile("${rootDir}/style/eclipse-java-google-style.xml")
  }
}
```
