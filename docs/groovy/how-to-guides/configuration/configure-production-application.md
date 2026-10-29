---
title: Configure the Deephaven production application
---

This guide describes the Deephaven production application's directory layout, its `start` script, and its bootstrap configuration. Bootstrap configuration is the small set of settings, such as the data, cache, and config directories, that the server reads at startup, before it reads any configuration file. Use this guide to change where Deephaven stores data, cache, and configuration files, to set the JVM heap and other startup options, and to find the directories the application ships with.

The Deephaven production application is the recommended way to run Deephaven in production, hence its name. It runs Deephaven directly from artifacts produced during each release. Download the artifacts from the [GitHub releases page](https://github.com/deephaven/deephaven-core/releases), under the assets list.

The production application gives you direct control over the server's JVM options, directories, and startup, but requires more setup and consideration of low-level details. Users who wish to get up and running without needing to worry about these details should consider [Docker](../../getting-started/docker-install.md).

This guide assumes familiarity with installing and running the Deephaven production application. For an introductory guide on installing and running, see [Install and run the Deephaven production application](../../getting-started/production-application.md).

## Prerequisites

The production application requires [Java](https://www.java.com/en/) 17 or later. Deephaven recommends Java 21, a long-term support (LTS) release. A Python console, the default, also requires [Python](https://www.python.org/) 3.9 or later. To run a Groovy console instead, see [Choose the console type](#choose-the-console-type). For the full list of requirements, see the [production application prerequisites](../../getting-started/production-application.md#prerequisites).

The production application also requires Linux or macOS. It does not run natively on Windows, so Windows users need Windows Subsystem for Linux v2 (WSL 2).

> [!WARNING]
> WSL 2's default time-sync setup can cause spurious 10–20-second clock jumps that stall Deephaven tables that update in real time. Before running Deephaven on WSL 2, apply one of the [time-sync workarounds](../../reference/community-questions/wsl2-clock-drift.md).

## Directory layout

The inner structure of the application is a root directory of the form `server-jetty-<version>/` with further subdirectories:

- `bin/`: Contains a [`start` script](../../getting-started/production-application.md#run-the-server) used to launch the server.
- `lib/`: Contains all of the JAR files that form the application classpath.
- `etc/`: Contains the Java Virtual Machine (JVM) option files and the compiler directives file (`dh-compiler-directives.txt`) that the `start` script always loads.

For the option file used for your Java version, see `JAVA_OPTS` under [Environment variables](#environment-variables).

## Configure the start script

### What the start script does

The [`start` script](../../getting-started/production-application.md#run-the-server) collects the arguments, assembles the classpath, and executes a shell command that looks something like:

```shell
java <required-opts> $JAVA_OPTS $START_OPTS -classpath <classpath> io.deephaven.server.jetty.JettyMain
```

- `<required-opts>` are JVM settings that the `start` script always passes before `JAVA_OPTS` and `START_OPTS`. You can't remove them without editing the script.
- A `-D` property in `START_OPTS` overrides a `-D` property of the same name in the script's default JVM options.
- `<classpath>` is the constructed classpath. You can't change it, but you can prepend entries with the `EXTRA_CLASSPATH` environment variable.

### Environment variables

The table below lists the environment variables that the `start` script and the server read. Set them in the shell environment before you run the `start` script. The server's bootstrap environment variables are in [Configure server directories](#configure-server-directories).

Set JVM system properties in `START_OPTS`. By default, set [Deephaven configuration properties](./configuration-properties.md) in a `deephaven.prop` file in the config directory. Environment variables can't be set in `deephaven.prop`.

| Environment variable | Description                                                                                                                                                                                                                                                                                            | Default value                                                        | Example                                              |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------- | ---------------------------------------------------- |
| `JAVA_OPTS`          | The main JVM arguments. If unset, the `start` script loads the Deephaven VM options file for your Java version: `etc/dh-default-<major>.vmoptions`, or `etc/dh-default.vmoptions` if no version-specific file exists. Setting `JAVA_OPTS` replaces those defaults, so Deephaven does not recommend it. | `-XX:VMOptionsFile="<install-dir>/etc/dh-default-<major>.vmoptions"` | N/A                                                  |
| `START_OPTS`         | Additional user-supplied JVM arguments. The start script adds these to the command line after `JAVA_OPTS`. Set these instead of `JAVA_OPTS`.                                                                                                                                                           | None                                                                 | `START_OPTS="-Xms4g -Xmx4g -Dmyproperty=myvalue"`    |
| `EXTRA_CLASSPATH`    | Additional directories to include in the Java classpath (e.g., `/apps/libs/*`)                                                                                                                                                                                                                         | None                                                                 | `EXTRA_CLASSPATH="/apps/libs/*:/opt/my_java_libs/*"` |

### Set the JVM heap size

The `-Xmx` option sets the maximum heap size. It is a JVM option, so set it through `START_OPTS`. For example, this command sets the maximum heap size to 8 GB for a single run:

```shell
START_OPTS="-Xmx8g" ./server-jetty-<version>/bin/start
```

### Choose the console type

To start a Groovy console, set `-Ddeephaven.console.type=groovy` through `START_OPTS`, for example:

```shell
START_OPTS="-Ddeephaven.console.type=groovy" ./server-jetty-<version>/bin/start
```

See [configuration properties](./configuration-properties.md) for the `deephaven.console.type` property.

## Configure server directories

### Deephaven server bootstrap configuration

The server reads its bootstrap configuration early in startup, before it reads any [configuration file](./config-file.md). Each bootstrap setting has a default value. To change one, set an environment variable or a system property. If both are set, the system property wins.

| Bootstrap configuration parameter | Environment variable    | System property         | Description                                                              | Default value |
| --------------------------------- | ----------------------- | ----------------------- | ------------------------------------------------------------------------ | ------------- |
| Application name                  | `DEEPHAVEN_APPLICATION` | `deephaven.application` | The name used to inform default values for other pieces of configuration | `deephaven`   |
| Data directory                    | `DEEPHAVEN_DATA_DIR`    | `deephaven.dataDir`     | The directory where users and applications read and write data           | OS-dependent  |
| Cache directory                   | `DEEPHAVEN_CACHE_DIR`   | `deephaven.cacheDir`    | The directory where the Deephaven server reads and writes cache data     | OS-dependent  |
| Config directory                  | `DEEPHAVEN_CONFIG_DIR`  | `deephaven.configDir`   | The directory where the Deephaven server reads configuration files       | OS-dependent  |
| Quiet flag                        | `DEEPHAVEN_QUIET`       | `deephaven.quiet`       | Whether to suppress bootstrap logging                                    | `false`       |

The server reads the `deephaven.prop` file from the config directory if that file exists. Otherwise, it reads the built-in `dh-defaults.prop`. To use a different file, set the `Configuration.rootFile` system property. For the full lookup order, see [How to create a Deephaven configuration file](./config-file.md).

Docker images set bootstrap values explicitly. The [Deephaven Docker images](./docker-application.md) run the production application with Docker-specific values, such as `DEEPHAVEN_DATA_DIR=/data`, `DEEPHAVEN_CACHE_DIR=/cache`, and `DEEPHAVEN_CONFIG_DIR=/opt/deephaven/config`.

#### Set directories for a single run

To set the data directory for a single run, use the `DEEPHAVEN_DATA_DIR` variable:

```shell
DEEPHAVEN_DATA_DIR=/srv/dh-data ./server-jetty-<version>/bin/start
```

To set the cache or config directory, use `DEEPHAVEN_CACHE_DIR` or `DEEPHAVEN_CONFIG_DIR` the same way:

```shell
DEEPHAVEN_CACHE_DIR=/srv/dh-cache ./server-jetty-<version>/bin/start
DEEPHAVEN_CONFIG_DIR=/srv/dh-config ./server-jetty-<version>/bin/start
```

To set the same directory as a system property, pass it in `START_OPTS`:

```shell
START_OPTS="-Ddeephaven.dataDir=/srv/dh-data" ./server-jetty-<version>/bin/start
```

#### OS-dependent directories

In these paths, `<application>` is the value of the Application name parameter (`deephaven` by default).

| OS    | Data directory                                                            | Cache directory                                              | Config directory                                                          |
| ----- | ------------------------------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------------------- |
| Linux | `$HOME/.local/share/<application>/`                                       | `$HOME/.cache/<application>/`                                | `$HOME/.config/<application>/`                                            |
| macOS | `$HOME/Library/Application Support/io.Deephaven-Data-Labs.<application>/` | `$HOME/Library/Caches/io.Deephaven-Data-Labs.<application>/` | `$HOME/Library/Application Support/io.Deephaven-Data-Labs.<application>/` |

On Linux, if `XDG_DATA_HOME`, `XDG_CACHE_HOME`, or `XDG_CONFIG_HOME` is set, the matching directory uses that variable's value in place of `$HOME/.local/share`, `$HOME/.cache`, or `$HOME/.config`.

## Related documentation

- [How to configure the Docker application](./docker-application.md)
- [How to create a Deephaven configuration file](./config-file.md)
- [Install and run the Deephaven production application](../../getting-started/production-application.md)
- [Install Deephaven with Docker](../../getting-started/docker-install.md)
- [Configuration properties](./configuration-properties.md)
- [Periodic update graph configuration](../../conceptual/periodic-update-graph-configuration.md)
