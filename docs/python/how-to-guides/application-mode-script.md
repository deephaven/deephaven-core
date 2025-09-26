---
title: Use Application Mode scripts
sidebar_label: Application Mode scripts
---

Deephaven's Application Mode allows you to [initialize server state](./application-mode.md). This guide covers how to use Application Mode scripts, allowing users to run Python or Groovy code when Deephaven is launched.

Application Mode scripts are defined by having `type=script` set in the Application Mode `*.app` [configuration file](../reference/app-mode/application-mode-config.md). Once defined, any Python or Groovy scripts in the `file_N` settings of the config file will run when Deephaven is launched.

Deephaven's [deephaven-examples Github organization](https://github.com/deephaven-examples) contains many examples of applications that use Application Mode. Specific tutorial examples can also be found for [Groovy](https://github.com/deephaven-examples/app-mode-init-groovy) and [Python](https://github.com/deephaven-examples/app-mode-init-python).

## Application Mode config file

Deephaven expects your Application Mode configuration to be defined in a [config file](../reference/app-mode/application-mode-config.md) that ends with `.app` (this is the `*.app` file). For using Application Mode with scripts, you need to set:

- `type=script`.
- The `scriptType` to `python` or `groovy`.
- `enabled=True`.
- Set `id` to an identifier of your choice.
- Set `name` to an application name of your choice.
- Your `file_N` files to run. Your `file_N` files can be any number of files defined relative to where the `*.app` file is located when packaged into Docker.

The following shows an example of a typical `.app` file:

```
type=script
scriptType=python
enabled=true
id=hello.world
name=Hello World!
file_0=./helloWorld.py
```

## Application Mode config files directory

When running Deephaven via Docker, you can set an additional [configuration property](./configuration/docker-application.md), `-Ddeephaven.application.dir`, to the directory containing your code listed in your [application mode configuration file](#application-mode-config-file).

The following Dockerfile builds on Deephaven's Python base Dockerfile by setting the application mode directory to `/app.d` within the Docker container.

```yml
services:
  deephaven:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -Ddeephaven.application.dir=/app.d
```

## Packaging your files

Although Deephaven doesn't have strict rules on where your `*.app` [config files](../reference/app-mode/application-mode-config.md) and your script files are located, it may be easier to put them in the same directory. This will simplify packaging your files with Docker, and will simplify your `file_N` paths in your `*.app` [config file](../reference/app-mode/application-mode-config.md).

In this example, we will show what a directory `app.d` will look like, and how to package it with a Deephaven Dockerfile.

### app.d directory

Let's say we have two Python scripts (`init_tables.py` and `functions.py`) we want to run when Deephaven is launched. We can make a directory called `app.d`, and put these scripts in there.

```
app.d
  init_tables.py
  functions.py
```

Now we need to define our `*.app` [config file](../reference/app-mode/application-mode-config.md) to run these Python scripts on launch. Let's name it `config.app` and use the following to define it.

```
type=script
scriptType=python
enabled=true
id=app-mode-init
name=App Mode Init
file_0=./init_tables.py
file_1=./functions.py
```

Once we put our `config.app` file in our `app.d` directory, it will look like this.

```
app.d
  init_tables.py
  functions.py
  config.app
```

### Dockerfile

Since we have bundled our scripts and [config files](../reference/app-mode/application-mode-config.md) in the same directory (`app.d`), our Dockerfile simply needs to copy over this directory when we build it. All we need to do is make our own Dockerfile that extends the Deephaven server image, and copies over our `app.d` directory.

This Dockerfile assumes that it is in the same directory as `app.d`.

```
FROM ghcr.io/deephaven/server:${VERSION:-latest}
COPY app.d /app.d
```

Because you're now extending Deephaven's Docker image, you will need to build this locally and tag it with an appropriate tag. You will also need to reference this tag in your `docker-compose.yml` file.

```
docker build --tag my-app/deephaven-grpc .
```

## Putting it all together

Once we've defined our files, our directory should look like this.

```
my-project
  Dockerfile
  docker-compose.yml
  app.d
    init_tables.py
    functions.py
    config.app
```

Our Dockerfile should contain the following.

```
FROM ghcr.io/deephaven/server
COPY app.d /app.d
```

Assuming we build and tag our Docker image with `docker build --tag my-app/deephaven-grpc .`, our `docker-compose.yml` file should look like this.

```yml
services:
  deephaven:
    image: ghcr.io/deephaven/deephaven-grpc:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -Ddeephaven.application.dir=/app.d
```

After building the Docker image, we can now launch the application by running `docker-compose up`.

## Example scripts

This section contains examples of code that can be included in a Python script. When configured, this code will run when Deephaven is launched.

```python order=my_table
from deephaven import empty_table


def hello() -> str:
    print("Hello world")


my_table = empty_table(5).update(["Value = i", "Hello = hello()"])
```

## A real world example

For a real world example of application mode being used in a Deephaven deployment, check out the [Deephaven Parquet viewer](https://github.com/devinrsmith/deephaven-parquet-viewer/tree/main), which uses Deephaven's application mode to view Parquet files from the command line.

## Related documentation

- [Application mode config file](../reference/app-mode/application-mode-config.md)
- [How to use Application Mode libraries](./application-mode-libraries.md)
- [Initialize server state with Application Mode](./application-mode.md)
