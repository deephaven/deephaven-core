---
title: Install and run with Docker
---

You can run Deephaven from pre-built Docker images without installing Java or Python yourself. This guide shows you how to run Deephaven from Docker, choose a deployment, and customize it for your applications.

> [!NOTE]
> Docker isn't the only way to run Deephaven. Users who wish to use Python without Docker should [install Deephaven with pip](./pip-install.md). Developers interested in tinkering with and modifying source code should [build Deephaven from source](./launch-build.md). Users who wish to run from build artifacts without Docker should [install and run the Deephaven production application](./production-application.md).

## Supported operating systems

Deephaven's Docker images run on the following operating systems:

- Linux
- macOS
- Windows 10 or 11 with [Windows Subsystem for Linux 2 (WSL 2)](https://learn.microsoft.com/en-us/windows/wsl/install)

> [!WARNING]
> WSL 2's default time-sync setup can cause spurious 10–20-second clock jumps that stall Deephaven [ticking tables](./crash-course/create-tables.md#ticking-tables). Before running Deephaven on WSL 2, apply one of the [time-sync workarounds](../reference/community-questions/wsl2-clock-drift.md).

## Prerequisites

Running Deephaven from Docker requires [`docker`](https://docs.docker.com/reference/cli/docker/) version 20.10.8 or later.

To run the `docker-compose.yml` files on this page, you also need [Docker Compose](https://docs.docker.com/compose/).

## The simplest possible installation

The following shell command downloads and runs the `server` image:

```sh
docker run --name deephaven -p 10000:10000 ghcr.io/deephaven/server:latest
```

Once the container is running, [open the Deephaven IDE](#open-the-deephaven-ide) and log in with the [pre-shared key](../how-to-guides/authentication/auth-psk.md) that Deephaven prints to the Docker logs. To set your own key, see [Set a pre-shared key](#set-a-pre-shared-key).

> [!WARNING]
> [`docker run`](https://docs.docker.com/reference/cli/docker/container/run/) creates a new container from an image every time you call it. To reuse a container that `docker run` created, use [`docker start`](https://docs.docker.com/reference/cli/docker/container/start/).

## Choose a deployment

Deephaven offers the following pre-built Docker images:

- `server`: Python
- `server-slim`: Groovy
- `server-nltk`: Python with [NLTK](https://www.nltk.org/)
- `server-pytorch`: Python with [PyTorch](https://pytorch.org/)
- `server-sklearn`: Python with [scikit-learn](https://scikit-learn.org/stable/)
- `server-tensorflow`: Python with [TensorFlow](https://www.tensorflow.org/)
- `server-all-ai`: Python with NLTK, PyTorch, scikit-learn, and TensorFlow

To run one of these images, replace `server` in the [`docker run`](https://docs.docker.com/reference/cli/docker/container/run/) command from [The simplest possible installation](#the-simplest-possible-installation) with that image's name. For example, `ghcr.io/deephaven/server-pytorch:latest` runs the `server-pytorch` image.

> [!NOTE]
> You can also install the Python packages in the images above yourself, like any other Python package. For more information, see [installing Python packages in Deephaven](../how-to-guides/install-and-use-python-packages.md).

Deephaven also publishes pre-built `docker-compose.yml` files in the [`containers` directory of the deephaven-core repository](https://github.com/deephaven/deephaven-core/tree/main/containers):

- `python/<variant>` runs one image from the list above. `<variant>` is `base` (the `server` image), `NLTK`, `PyTorch`, `SciKit-Learn`, `TensorFlow`, or `All-AI`.
- `python-examples/<variant>` adds [example data](https://github.com/deephaven/examples) to the same variants.
- `python-redpanda` adds [Redpanda](https://redpanda.com/).
- `python-examples-redpanda` adds both.

To use one, save its `docker-compose.yml` to an empty directory and [start the application](#start-the-application).

## Image versions

The examples below use the `latest` version. Deephaven recommends staying up to date, but you can pin an earlier image version if needed. The version can be any of the following:

- `latest` (default): The most recent release.
- A release tag, such as `41.7` or `42.6`: That specific release.
- `edge`: A nightly build that contains unreleased features.

Not every Deephaven release has a Docker image. To see which versions are available, check the [image tags on GitHub](https://github.com/deephaven/deephaven-core/pkgs/container/server).

To use a different version, replace `latest` in the image name. For example, `ghcr.io/deephaven/server:42.6` runs release 42.6.

Most `docker-compose.yml` examples on this page read the `VERSION` environment variable, which sets the image version and defaults to `latest`. For example, `VERSION=42.6 docker compose up` runs release 42.6.

The `python-examples/<variant>` files also run a second image, `ghcr.io/deephaven/examples`, which downloads the example data from GitHub into `./data/examples` the first time it runs. That image reads `VERSION` too, but its tags don't match Deephaven release numbers. To pin a Deephaven release with one of those files, edit the Deephaven `image:` line instead of setting `VERSION`.

## Modify the deployment

You can modify the Deephaven deployment with [Docker](https://www.docker.com/) alone or with [Docker Compose](https://docs.docker.com/compose/). Most subsections below show both ways. [Add a second image](#add-a-second-image) and [Build a custom image](#build-a-custom-image) use Docker Compose only. Deephaven recommends Docker Compose for custom deployments. To learn why, see [Key benefits of Docker Compose](https://docs.docker.com/compose/intro/features-uses/#key-benefits-of-docker-compose).

To modify the deployment with Docker alone, create the container with [`docker create`](https://docs.docker.com/reference/cli/docker/container/create/), and then run it with [`docker start`](https://docs.docker.com/reference/cli/docker/container/start/). Don't use `docker run` here (see the warning under [The simplest possible installation](#the-simplest-possible-installation)). Each `docker create` example below names the container `deephaven`. If a container with that name already exists, such as the one from [The simplest possible installation](#the-simplest-possible-installation), remove it first with `docker rm -f deephaven`, or pass a different `--name`.

To modify the deployment with Docker Compose, edit the `docker-compose.yml` file that creates the container. The examples below modify the following `docker-compose.yml` file. Save it in an empty directory. The examples use the `server` image. If you chose a different image in [Choose a deployment](#choose-a-deployment), keep that image name in place of `server`.

<details>
<summary>docker-compose.yml</summary>

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g
```

</details>

The `START_OPTS` environment variable passes JVM options to the Deephaven server. In this file, `-Xmx4g` sets the maximum heap size to 4GB. Some subsections below change this variable. Separate multiple options with spaces. The `docker create` examples set only the option they demonstrate, so add `-Xmx4g` to them if you want the same heap size as the Compose file.

`DEEPHAVEN_PORT` sets the host port and defaults to `10000`. [Change the port](#change-the-port) shows how to use it. `VERSION` sets the image version, as described in [Image versions](#image-versions).

After you run a `docker create` command or edit the file, see [Start the application](#start-the-application) to launch Deephaven.

### Set a pre-shared key

By default, Deephaven uses a [pre-shared key](../how-to-guides/authentication/auth-psk.md) to authenticate users. If you don't set a key, Deephaven generates a random one and prints it to the Docker logs.

The following deployment sets the pre-shared key to `YOUR_PASSWORD_HERE`.

```sh
docker create --name deephaven -p 10000:10000 --env START_OPTS=-Dauthentication.psk=YOUR_PASSWORD_HERE ghcr.io/deephaven/server:latest
```

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -Dauthentication.psk=YOUR_PASSWORD_HERE
```

### Disable authentication

[Anonymous authentication](../how-to-guides/authentication/auth-anon.md) allows anyone to access a Deephaven instance. The following deployment enables anonymous authentication.

```sh
docker create --name deephaven -p 10000:10000 --env START_OPTS=-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler ghcr.io/deephaven/server:latest
```

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler
```

### Add more memory

The following deployment sets the server's maximum heap size to 8GB.

```sh
docker create --name deephaven -p 10000:10000 --env START_OPTS=-Xmx8g ghcr.io/deephaven/server:latest
```

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx8g
```

### Change the port

The following deployment maps port `9999` on the host to Deephaven's port `10000` in the container, so you connect to Deephaven from your web browser on port `9999`.

```sh
docker create --name deephaven -p 9999:10000 ghcr.io/deephaven/server:latest
```

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-9999}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g
```

The example `docker-compose.yml` file from [Modify the deployment](#modify-the-deployment) reads the host port from the `DEEPHAVEN_PORT` environment variable, which defaults to `10000`. Instead of editing the file, you can keep it unchanged and run `DEEPHAVEN_PORT=9999 docker compose up`.

### Add a second volume

The example `docker-compose.yml` file mounts a local `data` directory at `/data` in the container. The following examples mount a local `specialty` directory at `/specialty`. The Compose version keeps the `data` mount as well:

```sh
docker create --name deephaven -p 10000:10000 -v "$(pwd)/specialty:/specialty" ghcr.io/deephaven/server:latest
```

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
      - ./specialty:/specialty
    environment:
      - START_OPTS=-Xmx4g
```

### Import custom JARs

You can make your own Java classes and third-party libraries available to queries by placing their JARs under `/apps/libs`. The images add `/apps/libs/*` to the JVM classpath at startup. For more ways to add JARs, see [Install and use Java packages](../how-to-guides/install-and-use-java-packages.md).

First, copy your JARs into a local `jars` directory:

```sh
mkdir -p jars
cp /path/to/<custom>.jar jars/
```

Then mount that directory at `/apps/libs`:

```sh
docker create --name deephaven -p 10000:10000 -v "$(pwd)/jars:/apps/libs" ghcr.io/deephaven/server:latest
```

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
      - ./jars:/apps/libs
    environment:
      - START_OPTS=-Xmx4g
```

Now you can run queries that use your custom JARs. For example, if you have a class `org.example.MathFns` with a static method `square(long x)`, you can run the following query:

```python skip-test
# Example usage (requires custom JAR with org.example.MathFns class)
from deephaven import empty_table

table = empty_table(5).update("Squares = org.example.MathFns.square(i)")
```

### Add a second image

Docker Compose specializes in running multi-container applications, so Deephaven recommends it for running a second image alongside Deephaven. The following YAML file runs Deephaven with [Redpanda](https://redpanda.com/).

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g

  redpanda:
    command:
      - redpanda
      - start
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
      - --schema-registry-addr internal://0.0.0.0:8081,external://0.0.0.0:18081
      - --smp 1
      - --memory 1G
      - --mode dev-container
    image: docker.redpanda.com/redpandadata/redpanda:v23.2.18
    ports:
      - 8081:8081
      - 18081:18081
      - 9092:9092
      - 19092:19092
```

### Build a custom image

Some custom deployments need changes built into the image itself, such as Python packages that every new container should have without a manual install. These deployments typically extend a Docker image with both a [Dockerfile](https://docs.docker.com/reference/dockerfile/) and a `docker-compose.yml` file.

The following subsections build a custom Deephaven image that adds Python packages the official Deephaven images don't include.

> [!NOTE]
> Put the `requirements.txt`, `Dockerfile`, and `docker-compose.yml` files in the same directory.

#### requirements.txt

`requirements.txt` lists the Python packages to install, one per line. For example:

```text
pendulum
polars
```

#### Dockerfile

A Dockerfile defines how to build a Docker image: the base image to start from and the steps that customize it.

The following Dockerfile takes the latest Deephaven `server` image and installs the Python packages defined in `requirements.txt` into the new image.

```Dockerfile
FROM ghcr.io/deephaven/server:latest
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt && rm /requirements.txt
```

To build on a specific release, replace `latest` in the `FROM` line with that release tag, such as `42.6`. The `VERSION` variable described in [Image versions](#image-versions) doesn't apply to an image you build yourself.

#### docker-compose.yml

Docker Compose can build a Docker image from a local Dockerfile. The following YAML file builds a Docker image from a Dockerfile named `Dockerfile` in the same directory and runs a container from it.

```yaml
services:
  deephaven:
    build: .
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g
```

## Start the application

To start a Deephaven application built from a `docker-compose.yml` file, run:

```sh
docker compose up --build
```

If your `docker-compose.yml` uses `build:`, as in [Build a custom image](#build-a-custom-image), the `--build` flag first rebuilds the image from your `Dockerfile`. If the file uses a pre-built `image:` instead, the flag has no effect.

> [!NOTE]
> If you've pulled a Deephaven image on this machine before, for example with `docker run` or an earlier [`docker compose up`](https://docs.docker.com/reference/cli/docker/compose/up/), run `docker compose up --build --pull always` instead so that you get the latest Docker images. Before running `docker create`, run `docker pull ghcr.io/deephaven/server:latest` for the same reason.

To start a container you created with [`docker create`](https://docs.docker.com/reference/cli/docker/container/create/), run [`docker start`](https://docs.docker.com/reference/cli/docker/container/start/) with the container's name:

```sh
docker start deephaven
```

## Open the Deephaven IDE

Once Deephaven is running, you can open the Deephaven IDE in your web browser. The Deephaven IDE allows you to interactively analyze data and develop new analytics.

- If Deephaven is running locally, navigate to [http://localhost:10000/ide/](http://localhost:10000/ide/).
- If Deephaven is running remotely, navigate to `http://<hostname>:10000/ide/`, where `<hostname>` is the address of the machine Deephaven is running on.

If you [changed the port](#change-the-port), replace `10000` with that port.

Unless you [disabled authentication](#disable-authentication), the IDE asks for a key when it opens. Enter the [pre-shared key](#set-a-pre-shared-key) you set. If you didn't set one, use the key that Deephaven prints to the container's logs. To see the logs, run [`docker logs deephaven`](https://docs.docker.com/reference/cli/docker/container/logs/), or [`docker compose logs`](https://docs.docker.com/reference/cli/docker/compose/logs/) for a Compose deployment.

![The Deephaven IDE upon startup](../assets/tutorials/launch/ide_startup.png)

## Find example data

The [Deephaven examples repository](https://github.com/deephaven/examples) contains datasets that help you learn Deephaven. Deephaven's documentation uses these datasets extensively, and some examples require them.

If you chose a [deployment](#choose-a-deployment) with example data, the example datasets appear in a `data/examples` directory next to your `docker-compose.yml` file. Inside the container, the same files are at `/data/examples`. See [Docker data volumes](../conceptual/docker-data-volumes.md) for more information on how Docker mounts files.

## Next steps

import { TutorialCTA } from '@theme/deephaven/CTA';

<div className="row">
<TutorialCTA to="/core/docs/getting-started/crash-course/overview" />
</div>

## Related documentation

- [Pre-shared key authentication](../how-to-guides/authentication/auth-psk.md)
- [Docker data volumes](../conceptual/docker-data-volumes.md)
- [Build and launch Deephaven from source code](./launch-build.md)
