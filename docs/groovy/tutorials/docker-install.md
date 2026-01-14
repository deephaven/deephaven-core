---
title: Install and run with Docker
sidebar_label: Docker
---

## Run Deephaven from Docker

Deephaven can be run from pre-built Docker images and requires only Docker. This guide will teach you how to run Deephaven from Docker, choose a deployment, and customize it for your applications.

> [!NOTE]
> Docker isn't the only way to run Deephaven. Developers interested in tinkering with and modifying source code should [build Deephaven from source](../how-to-guides/launch-build.md). Users who wish to run from build artifacts without Docker should use the [Deephaven production application](./production-application.md)

## Supported operating systems

Deephaven is only supported on:

- Linux
- MacOS
- Windows 10 or 11 (requires [WSL 2 (Windows Subsystem for Linux v2)](https://learn.microsoft.com/en-us/windows/wsl/install))

## Prerequisites

Running Deephaven from Docker requires [`docker`](https://docs.docker.com/reference/cli/docker/) >= 20.10.8.

[Docker Compose](https://docs.docker.com/compose/) is recomended for customized deployments, especially those that require multiple containers.

## The simplest possible installation

The following shell command downloads and runs the `server` image:

```sh
docker run --name deephaven -p 10000:10000 ghcr.io/deephaven/server-slim:latest
```

> [!WARNING]
> [`docker run`](https://docs.docker.com/reference/cli/docker/container/run/) creates a new container from an image every time it's called. To reuse a container created from [`docker run`](https://docs.docker.com/reference/cli/docker/container/run/), use [`docker start`](https://docs.docker.com/reference/cli/docker/container/start/).

> [!NOTE]
> This default configuration uses a [pre-shared key](../how-to-guides/authentication/auth-psk.md) to authenticate users. If not explicitly set, a randomly generated key gets printed to the Docker logs. See [set a pre-shared key](#set-a-pre-shared-key) for how to set your own key.

## Image versions

The `latest` version is used in the examples below. This corresponds to the most recent release number (e.g. `0.33.3`, `0.33.0`, etc.). While it's recommended to stay up-to-date with recent releases, Deephaven has many [releases](https://github.com/deephaven/deephaven-core/releases) that can be used if desired. Versions can be any of the following:

- `latest` (default): The most recent release.
- A specific release tag: `0.32.0`, `0.33.3`, etc.
- `edge`: An image published nightly and contains unreleased features.

## Modify the deployment

The Deephaven deployment can be modified through [Docker](https://www.docker.com/) alone or with [Docker Compose](https://docs.docker.com/compose/). The subsections below present ways to modify the deployment using both. Deephaven recommends the use of [Docker Compose](https://docs.docker.com/compose/) when creating custom deployments. See [Key benefits of Docker Compose](https://docs.docker.com/compose/intro/features-uses/#key-benefits-of-docker-compose) for more information on why.

Modifying the deployment with [Docker](https://www.docker.com/) should be done with [`docker create`](https://docs.docker.com/reference/cli/docker/container/create/). [`docker run`](https://docs.docker.com/reference/cli/docker/container/run/) will _always_ create a new container from an image. To run a pre-existing container, use [`docker start`](https://docs.docker.com/reference/cli/docker/container/start/).

Modifying the deployment with [Docker Compose](https://docs.docker.com/compose/) requires updating the `docker-compose.yml` file used to build the container. The examples below will modify the following `docker-compose.yaml` file.

<details>
<summary>docker-compose.yml</summary>

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g
```

</details>

### Set a pre-shared key

Deephaven, by default uses a [pre-shared key](../how-to-guides/authentication/auth-psk.md) to authenticate users looking to access Deephaven. If the key isn't set, Deephaven uses a randomly generated key that gets printed to the Docker logs.

The following deployment set the pre-shared key to `YOUR_PASSWORD_HERE`.

```sh
docker create --name deephaven -p 10000:10000 --env START_OPTS=-Dauthentication.psk=YOUR_PASSWORD_HERE ghcr.io/deephaven/server-slim:latest
```

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:${VERSION:-latest}
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
docker create --name deephaven -p 10000:10000 --env START_OPTS=-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler ghcr.io/deephaven/server-slim:latest
```

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler
```

### Add more memory

The following deployment tell the server to allocate 8GB of heap memory instead of the default of 4GB.

```sh
docker create --name deephaven -p 10000:10000 --env START_OPTS=-Xmx8g ghcr.io/deephaven/server-slim:latest
```

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx8g
```

### Change the port

The following deployment tell Deephaven to expose port `9999` for the user to connect to via their web browser.

```sh
docker create --name deephaven -p 9999:10000 ghcr.io/deephaven/server-slim:latest
```

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-9999}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g
```

### Add a second volume

Deephaven, by default, comes with a single `data` volume. The following deployment mounts a second `specialty` volume:

```sh
docker create --name deephaven -p 10000:10000 -v ./specialty:/specialty ghcr.io/deephaven/server-slim:latest
```

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
      - ./specialty:/specialty
    environment:
      - START_OPTS=-Xmx4g
```

### Add a second image

[Docker Compose](https://docs.docker.com/compose/) specializes in running multi-container applications. In fact, Deephaven used to run in four separate containers before being reduced to one. Adding a second image to a Docker application should _always_ be done with [Docker Compose](https://docs.docker.com/compose/). The following YAML file runs Deephaven with [Redpanda](https://redpanda.com/).

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:${VERSION:-latest}
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

Custom Docker deployments often require things that cannot be done in a YAML file or Docker command. For instance, it is not possible to install a Python package this way. Such deployments typically extend a Docker image using both a [Dockerfile](https://docs.docker.com/reference/dockerfile/) and a [docker-compose.yml](https://docs.docker.com/compose/) file.

The following subsections build a custom Deephaven application through Docker with several Python packages installed that do not ship with official Deephaven Docker images.

> [!NOTE]
> This example uses a flat directory structure - all files are placed in the same directory.

#### Dockerfile

A [Dockerfile](https://docs.docker.com/reference/dockerfile/) dictates which Docker images to build containers from and what else distinguishes these containers from their standard counterparts.

The following Dockerfile takes the latest Deephaven `server` image and installs the Python packages defined in `requirements.txt` into the container created from it.

```Dockerfile
FROM ghcr.io/deephaven/server-slim:latest
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt && rm /requirements.txt
```

#### docker-compose.yml

[Docker Compose](https://docs.docker.com/compose/) can be told to build a Docker image from a local Dockerfile. The following YAML file builds a Docker container from a Dockerfile named `Dockerfile` in the same directory.

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

#### Import custom JARs

You can make your own Java classes (and third-party libraries) available to queries by placing JARs under `/apps/libs`. The images add `/apps/libs/*` to the JVM classpath at startup.

<details>
<summary>Option A - Bind mount</summary>

```bash
$ mkdir -p jars
$ cp /path/to/<custom>.jar jars/
$ docker run --rm -p 10000:10000 -v "$(pwd)/jars:/apps/libs" ghcr.io/deephaven/server:latest
```

</details>

<details>
<summary>Option B - Docker Compose</summary>

```yaml title="docker-compose.yml"
services:
  deephaven:
    image: ghcr.io/deephaven/server:latest
    ports:
      - "10000:10000"
    volumes:
      - ./jars:/apps/libs
```

</details>

Now you can run queries that use your custom JARs. For example, if you have a class `org.example.MathFns` with a static method `square(long x)`, you can run the following query:

```python skip-test
# Example usage (requires custom JAR with org.example.MathFns class)
from deephaven import empty_table

table = empty_table(5).update("squares = org.example.MathFns.square(i)")
```

## Start the application

To start a Deephaven application built from a `docker-compose.yml` file, run:

```sh
docker compose up --build
```

The `--build` flag tells Docker to build the services specified by the `docker-compose.yml` file. The `Dockerfile` defines the custom installation process of the service.

> [!NOTE]
> If you've previously run `docker compose up`, add `--pull` to the command above to ensure you have the latest version of the Docker images.

## Run Deephaven IDE

Once Deephaven is running, you can launch a Deephaven IDE in your web browser. The Deephaven IDE allows you to interactively analyze data and develop new analytics.

- If Deephaven is running locally, navigate to [http://localhost:10000/ide/](http://localhost:10000/ide/).
- If Deephaven is running remotely, navigate to `http://<hostname>:10000/ide/`, where `<hostname>` is the address of the machine Deephaven is running on.

![The Deephaven IDE upon startup](../assets/tutorials/launch/ide_startup.png)

## Manage example data

The [Deephaven examples repository](https://github.com/deephaven/examples) contains data sets to help learn how to use Deephaven. Deephaven's documentation uses these data sets extensively, and they are needed to run some examples.

If you have chosen a deployment with example data, the example data sets will be downloaded to `data/examples` within your Deephaven folder, which translates to `/data/examples` within the Deephaven Docker container. See [Docker data volumes](../conceptual/docker-data-volumes.md) for more information on how files get mounted in Docker.

## What to do next?

import { TutorialCTA } from '@theme/deephaven/CTA';

<div className="row">
<TutorialCTA to="/core/groovy/docs/tutorials/crash-course/architecture-overview" />
</div>

## Related documentation

- [Pre-shared key authentication](../how-to-guides/authentication/auth-psk.md)
- [Docker data volumes](../conceptual/docker-data-volumes.md)
- [Build and launch Deephaven from source code](../how-to-guides/launch-build.md)
