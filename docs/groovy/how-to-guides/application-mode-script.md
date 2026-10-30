---
title: Use Application Mode scripts
---

Deephaven's Application Mode allows you to [initialize server state](./application-mode.md). This guide covers how to use Application Mode scripts, which run Python or Groovy code when Deephaven launches. The examples package the script in a Docker image. You can use the same `app.d` directory (the folder that holds your scripts and a `*.app` [config file](../reference/app-mode/application-mode-config.md)) with any launch that sets the [`deephaven.application.dir`](../reference/app-mode/application-mode-config.md) property. This guide shows the Docker setup.

## Application Mode config file

Deephaven reads settings for [Application Mode](./application-mode.md) from a [config file](../reference/app-mode/application-mode-config.md) whose name ends in `.app`. Call this file the `*.app` file. For using Application Mode with scripts, you need to set:

- `type` set to `script`
- `scriptType` set to `python` or `groovy`, matching the server's console language
- `id` set to an identifier of your choice
- `name` set to an application name of your choice
- `file_N` set to the path of one script, where `N` is an integer such as `0` or `1`. Scripts run in ascending numeric order.

`enabled` is optional and defaults to `true`. Set `enabled=false` to skip an application without deleting its file.

A complete example is in [Packaging your files](#packaging-your-files) below.

## Packaging your files

Deephaven doesn't require your script files to be in any particular place, as long as each `file_N` path points to them. Relative `file_N` paths resolve against the application directory (`app.d`), the directory that holds the `*.app` files. Absolute paths are used as they are.

Each `*.app` [config file](../reference/app-mode/application-mode-config.md) must sit directly in the `app.d` directory, not in a subdirectory. Putting the `*.app` and script files in the same directory simplifies packaging your files with Docker and simplifies your `file_N` paths.

In this example, we show what a directory `app.d` looks like and how to package it with a `Dockerfile` for Deephaven.

### Scripts and config file

Let's say we have a Groovy script (`init_tables.groovy`) we want to run when Deephaven launches. We can make a directory called `app.d`, and put the script in there.

```
app.d
  init_tables.groovy
```

The script creates `source`, a five-row table, and `result`, which adds `Value` and `Hello` columns to it. Each row's `Hello` value is the string that the `hello` closure returns.

```groovy order=result
hello = { -> "Hello world" }

source = emptyTable(5)
result = source.update("Value = i", "Hello = hello()")
```

Next, define our `*.app` [config file](../reference/app-mode/application-mode-config.md) to run this Groovy script on launch. Let's name it `config.app` and use the following to define it. Put it in the `app.d` directory next to the script.

```properties
type=script
scriptType=groovy
enabled=true
id=app-mode-init
name=App Mode Init
file_0=./init_tables.groovy
```

### Dockerfile

Since we have bundled our scripts and [config files](../reference/app-mode/application-mode-config.md) in the same directory (`app.d`), our `Dockerfile` needs to copy over this directory when we build it. The `Dockerfile` extends the Deephaven server image and copies over our `app.d` directory.

This `Dockerfile` assumes that it is in the same directory as `app.d`.

```dockerfile
ARG VERSION=latest
FROM ghcr.io/deephaven/server-slim:${VERSION}
COPY app.d /app.d
```

This example uses `server-slim`, which runs Groovy scripts and has no Python runtime. Python scripts need the `server` image.

Because you're extending Deephaven's Docker image, you need to build this locally and tag it, for example `my-app/deephaven-app`. You also need to reference this tag in your `docker-compose.yml` file. The build command is in [Putting it all together](#putting-it-all-together).

### docker-compose.yml

The `docker-compose.yml` file sets the [`deephaven.application.dir`](../reference/app-mode/application-mode-config.md) property to `/app.d`, the directory where the `Dockerfile` copies `app.d`. Pass the property through `START_OPTS`, as described in the [Docker application configuration guide](./configuration/docker-application.md).

The `ports` and `volumes` lines are optional. They publish the server port and mount a `data` folder. In the `environment` line, only `-Ddeephaven.application.dir=/app.d` is specific to Application Mode. `-Xmx4g` sets the JVM heap size and is optional.

Our `docker-compose.yml` file should look like this.

```yml
services:
  deephaven:
    image: my-app/deephaven-app
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -Ddeephaven.application.dir=/app.d
```

## Putting it all together

Once we've defined our files, our directory should look like this.

```
my-project
  Dockerfile
  docker-compose.yml
  app.d
    init_tables.groovy
    config.app
```

Build and tag the image with `docker build --tag my-app/deephaven-app .`, then run `docker compose up` to launch the application. When the server is up, open the Deephaven web IDE at `localhost:10000` in a browser. `result` and `source` are available in the query scope, each with five rows.

## Related documentation

- [Docker application configuration](./configuration/docker-application.md)
- [How to use Application Mode libraries](./application-mode-libraries.md)
- [Initialize server state with Application Mode](./application-mode.md)
- [Application Mode config file](../reference/app-mode/application-mode-config.md)
- [`update`](../reference/table-operations/select/update.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#emptyTable(long))
- [deephaven-examples GitHub organization](https://github.com/deephaven-examples)
- [Application Mode Groovy tutorial repository](https://github.com/deephaven-examples/app-mode-init-groovy)
- [How to use Application Mode video](https://youtu.be/GNm1k0WiRMQ)
