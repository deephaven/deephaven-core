---
title: Install packages
---

This guide discusses how to install packages for use within Deephaven. The Deephaven server runs on a Debian Docker container, and that Docker container can be modified to install custom packages.

## docker exec

In order to install packages for use within Deephaven, you need to install them on the `ghcr.io/deephaven/server` container. One of the easiest ways to do this is through `docker exec`.

With the default Deephaven `docker-compose.yml` file, `docker compose exec server` can be used to run commands on the `server` container. This example shows how to install the popular text editor Vim.

```
docker compose exec server apt-get update
docker compose exec server apt-get install vim -y
```

## Installing dependencies during build

The above example doesn't scale out for automated processes. In order to do this automatically, you'll need to create a custom Docker image that extends the base `ghcr.io/deephaven/server` image, and use that image for launching Deephaven.

The following custom `Dockerfile` installs Vim during its build.

```
FROM ghcr.io/deephaven/server

RUN apt-get update
RUN apt-get install vim -y
```

You'll then need to build this image, tag it with a name of your choice, and use that tag in your `docker-compose.yml` file. The following example shows how to do this with a tag named `deephaven/custom-server`.

```
docker build --tag deephaven/custom-server .
```

In the `docker-compose.yml`:

```
services:
  server:
    image: deephaven/custom-server
```

Once you've run these commands and updated your `docker-compose.yml` file, you can launch Deephaven with `docker compose up` and your custom dependencies will be installed.

### Using a shell script

For installing packages that require more than just `apt-get` commands (such as using a Makefile and having to change directories), you may want to use a shell script for the installation. This is very easy to do; simply create a shell script that is copied by the Dockerfile that contains the installation steps, and then run it. The following shows how to install Vim using a shell script.

#### `Dockerfile`

```
FROM ghcr.io/deephaven/server

COPY setup.sh /setup.sh
RUN sh /setup.sh
```

#### `setup.sh`

```
apt-get update
apt-get install vim -y
```

## Related documentation

- [How to install Java packages](./install-and-use-java-packages.md)
- [TA-lib sample app](https://github.com/jakemulf/ta-lib-install)<!--TODO: Change to Deephaven repo-->
