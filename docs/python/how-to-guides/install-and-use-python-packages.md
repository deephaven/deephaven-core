---
title: Install and use Python packages
---

This guide explains how to install Python packages for Deephaven and use them in queries. Where and how you install a package decides whether it persists:

- If you run Deephaven from a virtual environment with `pip` or from source code, packages persist across server restarts.
- If you install packages from a Python script or the Deephaven console, they persist in the environment where Deephaven runs. If Deephaven runs in a Docker container, a new container doesn't have them.
- If you install packages in a running Docker container, they stay in that container only. See [install in a running container](#install-packages-in-a-running-docker-container-from-the-command-line).
- If you build packages into a custom Docker image, every container started from it has them.

Once installed, a package can be imported and used like any other Python package. For an index of available Python packages and more information on them, visit the [Python Package Index](https://pypi.org/) or check out our [Choose Python packages to install](../reference/cheat-sheets/choose-python-packages.md) cheat sheet. This guide covers Python packages only. To install Unix packages in the Deephaven container, see [Install Unix packages in the Deephaven container](./install-packages.md).

## Install packages in a virtual environment

If you run Deephaven from a Python virtual environment, install packages into that environment with [`pip`](https://pip.pypa.io/). You might have created the environment by [installing Deephaven](../getting-started/pip-install.md) with pip or by [building it from source code](../getting-started/launch-build.md). Activate the environment, then run:

```shell
pip install pendulum
```

## Install packages from within a Python script

You can install Python packages with [`pip`](https://pip.pypa.io/) from within a Python script or the Deephaven Python console. The following code block installs the [`pendulum`](https://pypi.org/project/pendulum/) package into the same Python that runs Deephaven, then imports it.

```python order=null
import os
import shlex
import sys

os.system(f"{shlex.quote(sys.executable)} -m pip install pendulum")

import pendulum
```

The example ignores the exit status that `os.system` returns. If the install fails, the console shows only an `ImportError` on the `import pendulum` line, and pip's error text goes to the server's process output. To check the result and capture the error output, use the [`subprocess`](https://docs.python.org/3/library/subprocess.html) module instead.

Packages installed this way persist in the environment where Deephaven runs. If Deephaven runs in a Docker container, they have the same persistence limits as [packages installed in a running container](#install-packages-in-a-running-docker-container-from-the-command-line).

## Install packages in a running Docker container from the command line

Use [`docker exec`](https://docs.docker.com/reference/cli/docker/container/exec/) to run `pip install` in the running container.

> [!WARNING]
> Docker stores packages installed in a running container only in that container. They survive `docker stop` and `docker start`, but a new container doesn't have them. Running `docker run` again, or running `docker compose down` followed by `docker compose up`, creates a new container. To keep packages, [build a custom image](#permanently-add-packages-to-a-custom-docker-image).

The following command uses `docker exec` to run `pip install` in the running Deephaven container. The container name `deephaven` comes from `docker run --name deephaven`:

```shell
docker exec deephaven pip install pendulum
```

With Docker Compose, use the service name in place of the container name: `docker compose exec deephaven pip install pendulum`.

Once `pendulum` is installed in the container, you can run `import pendulum` in the Deephaven console. You don't need the `pip install` line from [the script example](#install-packages-from-within-a-python-script).

## Permanently add packages to a custom Docker image

To install packages that every new container has, create a custom Docker image and use it to start Deephaven. If you want to use a pre-built image, see [Choose a Deployment](../getting-started/docker-install.md#choose-a-deployment); pre-built images don't include packages you install here.

### Prerequisites

A custom image is built on top of a base image, which is the image Deephaven publishes. For this guide, the base image is `ghcr.io/deephaven/server:latest`. `docker build` downloads the base image if it isn't already on your machine, so you don't need to download it first.

You need Docker installed before you start. See the [Docker install guide](../getting-started/docker-install.md) for setup.

### Create a custom Dockerfile

To begin with, create a new directory. This directory should not be in a Deephaven deployment directory. You can name it whatever you'd like. For this guide, we use the name `deephaven-custom`.

```shell
mkdir deephaven-custom
cd deephaven-custom
```

Now, in this directory, create a file called `Dockerfile`. `Dockerfile` should use `ghcr.io/deephaven/server:latest` as the base image and contain a recipe for installing the new package. To add `pendulum`, the `Dockerfile` looks like this:

```Dockerfile
FROM ghcr.io/deephaven/server:latest
RUN pip install pendulum
```

To build on a specific release, replace `latest` in the `FROM` line with that release tag, such as `42.6`.

### Create a custom Docker image

Now that you have the `Dockerfile`, build the custom image. Run this command from the directory that contains the `Dockerfile`:

```shell
docker build --tag <user>/server-<custom> .
```

This creates a new Docker image called `<user>/server-<custom>`. For this guide, we call the image `guide/server-pendulum`:

```shell
docker build --tag guide/server-pendulum .
```

When the command finishes running, you can see the image in your system:

```shell
docker image ls
```

> [!CAUTION]
> A custom image keeps the base image it was built from. To include base image updates, rebuild the custom image with `docker build --pull --tag guide/server-pendulum .`.

### Start Deephaven from the custom image

If a container named `deephaven` already exists, you can remove it with `docker rm -f deephaven`, but only if it holds nothing you need. Removing it deletes packages installed in its writable layer. A new container doesn't reattach the existing `/data` and `/cache` volumes, so back up any data you need before you replace the container.

To start Deephaven from `guide/server-pendulum` with Docker, run:

```shell
docker run --name deephaven -p 10000:10000 guide/server-pendulum
```

With Docker Compose, change the `image` line of the `deephaven` service in your `docker-compose.yml` file to use `guide/server-pendulum`:

```yaml
services:
  deephaven:
    image: guide/server-pendulum:latest
```

The [Docker install guide](../getting-started/docker-install.md#build-a-custom-image) shows a Compose file that builds the custom image from a `Dockerfile` instead.

When you start Deephaven from `guide/server-pendulum`, you can use `pendulum`.

## List the modules Python can import

After you install a package, you can confirm that Python can import it. Python's [`help`](https://docs.python.org/3/library/functions.html#help) function can list the modules Python can import in the current session. The list includes the standard library and installed packages. Run the following command from a Python session:

```python syntax
help("modules")
```

## Use packages in query strings

You can use installed Python packages in [query strings](./query-string-overview.md). Query strings are the formulas you pass to [table operations](../conceptual/table-operations-overview.md), such as [`update`](../reference/table-operations/select/update.md). This example uses the `pendulum` package. You can install it in any of the ways described above.

<!-- Maintainer: skip-test because the snapshot test server doesn't have pendulum installed, so this block fails there. Remove skip-test once the test environment installs pendulum. -->

```python skip-test ticking-table order=times_in_paris
import pendulum
from deephaven import time_table


def get_paris_time():
    return pendulum.now("Europe/Paris")


times_in_paris = time_table("PT5S").update(formulas=["Paris = get_paris_time()"])
```

The example calls `pendulum` from a Python function, which [query strings](./query-string-overview.md) can call directly. For how Deephaven types these results and the performance cost of Python functions, see [Python functions in query strings](./python-functions.md).

<LoopedVideo src='../assets/how-to/pendulum_timeTableExample.mp4' />

## Related documentation

- [Table operations overview](../conceptual/table-operations-overview.md)
- [An overview of query strings](./query-string-overview.md)
- [Python functions in query strings](./python-functions.md)
- [Install Unix packages in the Deephaven container](./install-packages.md)
- [Create a time table](./time-table.md)
- [Launch Deephaven from pre-built images](../getting-started/docker-install.md)
- [Build and launch Deephaven from source code](../getting-started/launch-build.md)
- [Install Deephaven with pip](../getting-started/pip-install.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [Choose Python packages to install](../reference/cheat-sheets/choose-python-packages.md)
- [`time_table` Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.time_table)
- [`Table.update` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.update)
- [Access your file system with Docker bind mounts](../conceptual/docker-data-volumes.md)
