---
title: Create a Deephaven configuration file
---

A Deephaven configuration file is a [property file](https://en.wikipedia.org/wiki/.properties) that sets the properties the Deephaven server reads at startup. This page shows how to create one, where the server looks for it, and how its settings combine with the defaults.

A configuration file sets Deephaven properties. It can't change Java Virtual Machine (JVM) arguments (such as `-Xmx`) or JVM system properties that the JVM reads at startup (such as `java.io.tmpdir`). To set those, see the [production application script](./configure-production-application.md#environment-variables).

## Create the file

To start from the default values in the built-in `dh-defaults.prop` file, add an `includefiles` line that names it. For the properties that file sets, see [Configuration properties](./configuration-properties.md). The `includefiles` line must be the first non-comment line in the file:

```properties
includefiles=dh-defaults.prop

# An example to change the cycle time from the default of 1 second (1000 milliseconds) to 250 milliseconds
PeriodicUpdateGraph.targetCycleDurationMillis=250
```

If the file doesn't include `dh-defaults.prop`, the server doesn't load the values that file sets. Properties the server reads without a fallback in code must be set, either in your file or with a `-D` system property.

Save the file as `deephaven.prop` in `<configDir>`, the configuration directory. For where `<configDir>` is and how to change it, see [Deephaven server bootstrap configuration](./configure-production-application.md#deephaven-server-bootstrap-configuration).

## Where the server looks for the file

The server uses the first of these that applies:

1. The file named by the `Configuration.rootFile` system property. A system property is a Java property set on the command line with `-D`, so you set this one with `-DConfiguration.rootFile=<path>`. With the production start script, pass it in `START_OPTS`, for example `START_OPTS="-DConfiguration.rootFile=<path>"`.
2. The `<configDir>/deephaven.prop` file, if it exists.
3. The built-in `dh-defaults.prop` file.

## How settings combine

Properties later in the file override earlier ones, unless the earlier one is fixed with the `final` keyword, which the end of this section describes. The properties you add after `includefiles` override the defaults from `dh-defaults.prop`.

A system property set with `-D` on the command line overrides the same property in the configuration file. If that property is fixed with `final` (covered at the end of this section), the `-D` setting causes a configuration error instead.

You can mark a property `final` to fix its value. To do so, use one of these methods:

- Prefix its line with `final`.
- Put `finalize <property name>` on its own line.

For example, `final PeriodicUpdateGraph.targetCycleDurationMillis=250` fixes the cycle time at 250 milliseconds. Any later line in the file that sets a final property, including another `final` line or a plain assignment, causes a configuration error.

## Related documentation

- [Configure the production application](./configure-production-application.md)
- [Configuration properties](./configuration-properties.md)
