---
title: Install and use Python packages
sidebar_label: Python packages
---

This guide discusses how to install and use Python packages in Deephaven. Packages can be installed programmatically for use in a Deephaven instance or added to Deephaven Docker images to be available every time Deephaven is launched.

If you wish to do machine learning in Python, Deephaven has several pre-built Docker images for AI in Python. Refer to the [Choose a Deployment](../getting-started/docker-install.md#choose-a-deployment) section of the [Docker install guide](../getting-started/docker-install.md) to see what's currently available.

Once installed, a package can be imported and used like any other Python package. For an index of available Python packages and more information on them, visit the [Python Package Index](https://pypi.org/) or check out our [choosing the right Python package](../reference/cheat-sheets/choose-python-packages.md) cheat sheet.

## List all available Python packages

Python's `help` function can list the packages available to Python. Run the following command from a Python session:

```python
help("modules")
```

## Install packages from within a Python script

Python packages can be installed using [`pip`](https://www.datacamp.com/tutorial/pip-python-package-manager) from within a Python script or the Deephaven Python console. The following code block imports the [`Pendulum`](https://pypi.org/project/pendulum/) package and uses it to print the current time in Paris:

```python order=null
import os

os.system("pip install Pendulum")

import pendulum

now = pendulum.now("Europe/Paris")
print(now)
```

> [!WARNING]
> Packages installed this way are not guaranteed to persist across restarts of the Deephaven Docker container. If the container changes at all between restarts, packages _will not_ persist.

The [`subprocess`](https://docs.python.org/3/library/subprocess.html) module can also be used to save the output in case the installation fails.

```python order=null
from subprocess import Popen, PIPE

proc = Popen(["pip", "install", "nonexistent-package"], stdout=PIPE, stderr=PIPE)
stdout, stderr = proc.communicate()

print(stderr)
```

## Install packages in a running Docker container from the command line

Python packages can be installed using [`pip`](https://www.datacamp.com/tutorial/pip-python-package-manager) via [`docker exec`](https://docs.docker.com/engine/reference/commandline/exec/).

> [!WARNING]
> If this method is used with Deephaven Docker images, Python package installs do not persist after the Docker container exits. The package installation must be repeated each time the container is started.

Here [`docker exec`](https://docs.docker.com/engine/reference/commandline/exec/) is used to run a [`pip`](https://www.datacamp.com/tutorial/pip-python-package-manager) install on the running Deephaven Docker image, which is named `deephaven`.

```shell
docker exec deephaven pip install Pendulum
```

After installing the Pendulum package, we can use it within our script to print the current time in Paris.

```python
import pendulum

now = pendulum.now("Europe/Paris")

print(now)
```

## Permanently add packages to a custom Docker image

To import packages and have the installations persist between sessions, you can create a custom Docker image and then use that image to start Deephaven. Let's start with the steps that are common between both.

### Prerequisites

You must acquire the necessary base images before a custom Docker image can be built. This process differs based on how you launch Deephaven:

- If you [launch from pre-built images](../getting-started/docker-install.md), ensure you have run the following command to download the necessary base images:

```shell
docker compose pull
```

- If you [launch from source code](../getting-started/launch-build.md), ensure you have built the project with the necessary base images.

### Create a custom Dockerfile

To begin with, create a new directory. This directory should not be in a Deephaven deployment directory. You can name it whatever you'd like. For this guide, we'll name ours `deephaven-custom`.

```shell
mkdir deephaven-custom
cd deephaven-custom
```

Now, in this directory, create a file called `Dockerfile`. `Dockerfile` should use `ghcr.io/deephaven/server` as the base image and contain a recipe for installing the new package. When adding Pendulum, it looks like this:

```
FROM ghcr.io/deephaven/server
RUN pip3 install pendulum
```

### Create a custom Docker image

Now that we have the `Dockerfile` in place, we need to create the custom Docker image. To do so, run a command from the directory with `Dockerfile` that looks like:

```shell
docker build --tag <user>/server-<custom> .
```

This will create a new Docker image called `<user>/server-<custom>`. For this guide, we will call the image `guide/server-pendulum`:

```shell
docker build --tag guide/server-pendulum .
```

When the command finishes running, you can see the new image in your system:

```shell
docker image ls
```

### Reference the new image

To put it all together, we now need to reference this new image in the `docker-compose` file we use to launch Deephaven. The file name depends on how you build and launch Deephaven:

- If you [launch from pre-built images](../getting-started/docker-install.md), the file is `docker-compose.yml`, and can be found in your `deephaven-deployment` directory.

- If you [launch from source code](../getting-started/launch-build.md), the file is `docker-compose-common.yml`, and can be found in your `deephaven-core` directory.

In the Docker Compose file, see three lines of text that look like:

```
services:
  server:
    image: <IMAGE_NAME>
```

The image used by default depends on how you build and launch Deephaven. Regardless, you need to insert your custom image name in this line. Modify the `image` line to use your new image:

```
services:
  server:
    image: guide/server-pendulum:latest
```

Now, when you launch Deephaven again, you can use the package!

> [!CAUTION]
> When base images are updated by rebuilding source code or redownloading pre-built images, custom images must be rebuilt to incorporate the base image changes.

## Use Python packages in Deephaven

Once installed, a package can be imported and used like any other Python package. For an index of available Python packages and more information on Python packages, visit the [Python Package Index](https://pypi.org/) and our [Choose Python packages](../reference/cheat-sheets/choose-python-packages.md) cheat sheet.

## Use packages in query strings

Installed Python packages can be used in query strings. Let's use the Pendulum package, which we [installed](#install-packages-from-within-a-python-script) earlier in this guide.

```python syntax
import pendulum
from deephaven import time_table


def get_paris_time():
    return pendulum.now("Europe/Paris")


times_in_paris = time_table("PT5S").update(formulas=["Paris = get_paris_time()"])
```

<LoopedVideo src='../assets/how-to/pendulum_timeTableExample.mp4' />

## `deephaven.learn`

Python packages are commonly used with [`deephaven.learn`](./use-deephaven-learn.md). The [`deephaven.learn`](./use-deephaven-learn.md) package provides utilities for efficient data transfer between Deephaven tables and Python data structures, particularly [NumPy arrays](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html) and other data structures used by popular AI and machine learning packages.

## `deephaven.numpy`

The [`deephaven.numpy`](./use-numpy.md) module contains functions that convert between Deephaven tables and [NumPy arrays](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html). [NumPy](https://numpy.org/) is commonly used in Deephaven Python queries.

## `deephaven.pandas`

The [`deephaven.pandas`](./use-pandas.md) module contains functions that convert between Deephaven tables and [Pandas DataFrames](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html). [Pandas](https://pandas.pydata.org/docs/index.html) is another package that is commonly used in queries.

## Related documentation

- [How to install packages](./install-packages.md)
- [Choose Python packages to install](../reference/cheat-sheets/choose-python-packages.md)
- [Launch Deephaven from pre-built images](../getting-started/docker-install.md)
- [Build and launch Deephaven from source code](../getting-started/launch-build.md)
- [Access your file system with Docker data volumes](../conceptual/docker-data-volumes.md)
- [Create a time table](./time-table.md)
- [Use `deephaven.learn`](./use-deephaven-learn.md)
- [Use `deephaven.numpy`](./use-numpy.md)
- [Use `deephaven.pandas`](./use-pandas.md)
