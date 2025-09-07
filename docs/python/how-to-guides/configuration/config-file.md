---
title: Create a Deephaven configuration file
sidebar_label: Deephaven configuration files
---

The Deephaven configuration file is a [property file](https://en.wikipedia.org/wiki/.properties) that enables the user to configure different aspects of the Deephaven server.

By default, the Deephaven server sources an internal `dh-defaults.prop` file if no explicit configuration file is provided.

The recommended approach to providing a configuration file is to create and populate the file `<configDir>/deephaven.prop`[^1]. See [config directory](./configure-production-application.md#deephaven-server-bootstrap-configuration) for more details on `<configDir>`.

It is recommended that configuration files extend from `dh-defaults.prop` through the use of the `includefiles` configuration property:

```properties
includefiles=dh-defaults.prop

# An example to change the cycle time from the default of 1 second to 250 milliseconds
PeriodicUpdateGraph.targetCycleDurationMillis=250
```

Configuration properties that come later in the configuration file document will override configuration properties from earlier in the document.

Configuration files inherit system properties, but the converse is not true: you are not able to set system properties, nor JVM arguments, with the configuration file. See the [production application script](./configure-production-application.md#what-the-start-script-does) for details on setting system properties or JVM arguments.

## Related documentation

- [Configure the production application](./configure-production-application.md)
- [Configure the Docker application](./docker-application.md)

[^1]: The user can also set the configuration file with the system property `Configuration.rootFile`, but the preferred approach is to use `<configDir>/deephaven.prop`.
