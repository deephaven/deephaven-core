---
title: Install OS packages in the Deephaven Docker image
---

This guide shows how to add operating-system packages to the `ghcr.io/deephaven/server-slim` Docker image, either temporarily in a running container or permanently in a custom image. The `server-slim` image runs Deephaven with a Groovy console. The `server` image runs a Python console instead. See [Choose a deployment](../getting-started/docker-install.md#choose-a-deployment) for all the pre-built images.

These steps apply to the Deephaven Docker images. The images are based on Ubuntu 24.04, so you install these packages with `apt-get`. Outside Docker, install OS packages with your host's package manager.

> [!NOTE]
> This guide covers OS packages. To install Java packages, see [How to install and use Java packages](./install-and-use-java-packages.md).

## Install packages in a running container

To install packages for use within Deephaven, run the install commands in the `ghcr.io/deephaven/server-slim` container with `docker compose exec`.

With the default Deephaven `docker-compose.yml` file, `docker compose exec deephaven` runs commands in the `deephaven` service's container. See [Modify the deployment](../getting-started/docker-install.md#modify-the-deployment) for that file. This example shows how to install the popular text editor Vim.

```bash
docker compose exec deephaven apt-get update
docker compose exec deephaven apt-get install vim -y
```

Docker discards packages installed this way when it recreates the container (for example, after `docker compose down`). To keep them, build a custom image as shown in the next section.

## Install packages in a custom image

The [`docker compose exec`](#install-packages-in-a-running-container) method doesn't suit automated deployments, because each new container needs you to run the commands again. To install packages at build time, create a custom Docker image that extends the base `ghcr.io/deephaven/server-slim` image, and launch Deephaven from that image.

The following custom `Dockerfile` installs Vim during its build.

```dockerfile
FROM ghcr.io/deephaven/server-slim:latest

RUN apt-get update && apt-get install -y vim && rm -rf /var/lib/apt/lists/*
```

Build this image in the directory that contains the `Dockerfile`, and tag it with a name of your choice. The following example uses a tag named `deephaven/custom-server`.

```bash
docker build --tag deephaven/custom-server .
```

In the `docker-compose.yml` file, set the `image` of the `deephaven` service:

```yaml
services:
  deephaven:
    image: deephaven/custom-server
```

Once you've run these commands and updated your `docker-compose.yml` file, launch Deephaven with `docker compose up`, and the container includes your custom packages.

For reproducible builds, pin a release tag such as `42.7` in the `FROM` line instead of `latest`.

### Using a shell script

Some packages need more than a single `apt-get` command to install. For example, the install might change directories or run a `Makefile`. For these packages, you can put the installation steps in a shell script instead of the inline `RUN` line in the `Dockerfile` above. The `Dockerfile` copies the script into the image and runs it. Build the image and update `docker-compose.yml` as shown in [Install packages in a custom image](#install-packages-in-a-custom-image).

The following example shows the script form with a multi-step install. It installs `htop`, then changes directories and creates a shorter command name for it.

#### `Dockerfile`

```dockerfile
FROM ghcr.io/deephaven/server-slim:latest

COPY setup.sh /setup.sh
RUN sh /setup.sh
```

#### `setup.sh`

```sh
set -e
apt-get update
apt-get install -y htop
rm -rf /var/lib/apt/lists/*
cd /usr/local/bin
ln -s /usr/bin/htop h
```

## Related documentation

- [Install and run with Docker](../getting-started/docker-install.md)
- [How to install and use Java packages](./install-and-use-java-packages.md)
