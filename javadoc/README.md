# javadoc doclet

This module builds the doclet that our javadoc building process uses.

To support [Searchable Javadocs](https://github.com/deephaven/deephaven-core/issues/590)
we need to have a Java 11 doclet and Java 11 javadoc process.

The majority of developers don't need to build javadocs locally.
In an effort to minimize build dependencies, the javadoc module is disabled by default.

### Development

To aid in the doclet development processes, a developer can temporarily set the gradle property
`includeJavadocs` to true in [gradle.properties](../gradle.properties).
This should enable IDEs to pick up the module.

Alternatively, the setting can be enabled ad-hoc to build the javadocs:

```shell
./gradlew -PincludeJavadoc=true allJavadoc
```

This is essentially what happens in CI, see [docs-ci.yml](../.github/workflows/docs-ci.yml).
