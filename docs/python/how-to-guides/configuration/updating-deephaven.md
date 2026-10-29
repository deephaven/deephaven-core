---
title: Update Deephaven
---

This guide shows you how to update your Deephaven deployment.

Each Deephaven release adds features and fixes. Keep your deployment up to date. The steps depend on how you install and run Deephaven, so go to the section for your installation method or client.

> [!NOTE]
> Read the [release notes](https://github.com/deephaven/deephaven-core/releases) before you update across a [major version](#deephaven-versioning).

## Deephaven versioning

Each Deephaven version is two numbers separated by a period, such as `42.7`. A major release increments the first number and resets the second, for example from `41.7` to `42.0`. Other releases increment the second number, for example from `42.6` to `42.7`.

Sometimes Deephaven keeps patching an older line. For example, `41.11` was released after `42.0`, so a higher second number does not always mean a newer release. The `latest` tag points to the current release, and the [Deephaven releases page](https://github.com/deephaven/deephaven-core/releases) lists every release.

## Before you update

- Note the version you run now, so you can roll back by returning to it. For example, set the previous image tag, or run `pip install -U deephaven-server==<old version>`.
- Back up your Deephaven data directory (in Docker, `/data` or the mount or volume that holds it).

## Update Docker-installed Deephaven

Docker keeps a copy of each image tag it has already pulled and reuses it. Moving tags such as `latest` and `edge` can be stale, so pull them again before you recreate the container. A version tag you have not pulled yet downloads automatically with `docker run` and `docker compose up`.

### Set the image version

When you run [Deephaven from Docker](../../getting-started/docker-install.md), you choose the image version with a tag after the image name. You set the tag the same way with `docker run`, a `docker-compose.yml` file, or a `Dockerfile`. The tag is one of the following:

- A specific version number, such as `42.7`. Not every release has an image, so check the [image tags on GitHub](https://github.com/deephaven/deephaven-core/pkgs/container/server) first.
- The `latest` tag, which is the most recent Deephaven release.
- The `edge` tag, which is a regularly rebuilt image of the [deephaven-core](https://github.com/deephaven/deephaven-core) `main` branch, built at least nightly.

> [!NOTE]
> Most deployments use a specific version number or `latest`. Use `edge` only for testing, because it includes unreleased changes.

With `docker run`, the tag follows the image name:

```bash
docker run --name deephaven -p 10000:10000 ghcr.io/deephaven/server:42.7
```

If you deploy with Docker Compose, set the version in the `image` line of the Deephaven service in `docker-compose.yml`:

```yaml
image: ghcr.io/deephaven/server:42.7
```

In Docker Compose, you can also read the version from an environment variable:

```yaml
image: ghcr.io/deephaven/server:${VERSION:-latest}
```

This line uses the value of the `VERSION` environment variable, or `latest` if `VERSION` is not set.

In a `Dockerfile`, put the tag after the image name in the `FROM` line:

```dockerfile
FROM ghcr.io/deephaven/server:42.7
```

### Pull and recreate

> [!WARNING]
> Before you remove a container, make sure `/data`, the Deephaven data directory in the container, is in a bind mount or named volume. Data in a mount persists when you recreate the container.
>
> Without a mount, `/data` is an anonymous volume tied to that container:
>
> - With `docker run`, a new container starts with an empty `/data`.
> - The old data stays in an unreferenced volume.
> - Docker deletes the old data if you remove the container with `docker rm -v` or run it with `--rm`.
>
> To mount the data directory:
>
> - With `docker run`, add `-v "$(pwd)/data":/data`.
> - With Docker Compose, add a `volumes:` entry.

After you set the new tag:

1. Pull the image with `docker compose pull`, or with `docker pull ghcr.io/deephaven/server:<tag>`. If you use a different image, such as `server-pytorch`, use its name in place of `server`.
2. Recreate the container with `docker compose up -d`, or stop and remove the old container (`docker stop deephaven` then `docker rm deephaven`), then run `docker run` again.

If you build your own image from a Dockerfile, change the `FROM` tag and rebuild with `docker build` or `docker compose build --pull`. Then recreate the container.

## Update pip-installed Deephaven

Update [pip-installed Deephaven](../../getting-started/pip-install.md) with [`pip`](https://packaging.python.org/en/latest/tutorials/installing-packages/) by adding the `-U` or `--upgrade` flag.

To update to the latest version:

```bash
pip install -U deephaven-server
```

To update to a specific version, add `==` and the version number:

```bash
pip install -U deephaven-server==42.7
```

Then restart the server.

## Update the Python client

Update the [Deephaven Python client](/core/client-api/python/) when you update the server, using [`pip`](https://packaging.python.org/en/latest/tutorials/installing-packages/):

```bash
pip install -U pydeephaven
```

To update to a specific version, add `==` and the version number, such as `pydeephaven==42.7`.

## Update a source build

1. In your `deephaven-core` clone, run `git fetch --tags`.
2. Check out the release tag or branch you want, such as `git checkout v42.7`.
3. Rebuild and restart the server using the [build from source](../../getting-started/launch-build.md) guide.

## Update the production application

To update the [production application](../../getting-started/production-application.md), download and unpack the artifacts for the version you are updating to. Install the matching `deephaven-core` wheel in your virtual environment. Then restart the server.

## Related documentation

- [Configure the Deephaven Docker application](./docker-application.md)
- [Install guide for Docker](../../getting-started/docker-install.md)
- [Install guide for pip](../../getting-started/pip-install.md)
- [Build and run Deephaven from source code](../../getting-started/launch-build.md)
- [Install and run the Deephaven production application](../../getting-started/production-application.md)
- [Python client](/core/client-api/python/)
