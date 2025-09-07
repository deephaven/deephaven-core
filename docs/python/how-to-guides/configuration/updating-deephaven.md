---
title: Update Deephaven
---

This guide will show you how to update your Deephaven deployment.

It is typically recommended to keep up-to-date with the latest versions of software. Deephaven is no different. Each release brings new features, fixes, and quality-of-life improvements to the Deephaven experience. Updating is a quick and easy process, though the steps differ slightly depending on how you install and run Deephaven:

- [Docker-installed Deephaven instructions](#update-docker-installed-deephaven)
- [pip-installed Deephaven instructions](#update-pip-installed-deephaven)
- [Python client instructions](#update-the-python-client)

## Deephaven versioning

Each new Deephaven release increases the version number. Versions follow a nomenclature of three numbers separated by periods (e.g. `0.26.1`).

Deephaven's release and version history can be found [here](https://github.com/deephaven/deephaven-core/releases).

A major update is released approximately once per month, with occasional spot releases in between to roll out bug fixes and improvements to new features. Both release types are driven primarily by our user base.

Primary releases bump the middle of the three numbers (e.g., `0.25.0` -> `0.26.0`), whereas spot releases bump the last number (e.g., `0.26.0` -> `0.26.1`).

## Update Docker-installed Deephaven

When using [Deephaven run from Docker](../../getting-started/docker-install.md), a user will build the application from a `docker-compose.yml` file or a `Dockerfile`. In both cases, you can specify a version via one of the following:

- A specific version number, e.g. `0.26.1`.
- `latest`: The latest version to be released. The first release shown in the [releases](https://github.com/deephaven/deephaven-core/releases) page should always have this tag.
- `edge`: Everything merged into [deephaven-core](https://github.com/deephaven/deephaven-core) prior to midnight ET (US Eastern Time) on the previous day.

> [!NOTE]
> Most users will use a specific version number or `latest` for their deployment. The `edge` version is typically used for internal testing, as it has undocumented features not included in the latest release.

If deploying with `docker compose`, the `docker-compose.yml` file specifies the version based on the image in the Deephaven service:

```yaml
image: ghcr.io/deephaven/server:0.26.1
```

The version is set at the end of this line, after the `:`. In this case, the version is `0.26.1`, but it could be any other release number, `latest`, or `edge`.

The file above creates the `deephaven` docker image from the Deephaven `server` container. The version is specified at the end of the `image` line. In this case, it sets the image to the environment variable `VERSION`. If `VERSION` is not set, it uses `latest`.

The version can also be set with an environment variable:

```yaml
image: ghcr.io/deephaven/server:${VERSION:-latest}
```

In this case, the version is equal to the `VERSION` environment variable. If it is not set, `latest` is used.

If deploying with a `Dockerfile`, the syntax remains the same. The version is set after the image from which the container is built, separated by the `:` character.

## Update pip-installed Deephaven

[pip-installed Deephaven](../../getting-started/pip-install.md) can be updated with [`pip`](https://packaging.python.org/en/latest/tutorials/installing-packages/) by adding the `-U` or `--upgrade` flag.

The command below will update to the latest version.

```bash
pip install -U deephaven-server
```

To update to a specific version, use `==` to denote the version number.

```bash
pip install -U deephaven-server==0.26.0
```

## Update the Python client

The same rules apply to the [Deephaven Python client](/core/client-api/python/).

```bash
pip install -U pydeephaven
pip install -U pydeephaven==0.26.0
```

## Related documentation

- [How to configure the Deephaven Docker application](./docker-application.md)
- [Install guide for Docker](../../getting-started/docker-install.md)
- [Install guide for pip](../../getting-started/pip-install.md)
