---
title: Configure JS plugins
---

The Deephaven server supports custom JavaScript (JS) plugins that extend the functionality of the [web client UI](https://github.com/deephaven/web-client-ui). The server serves them to the browser.

This guide shows you how to install JS plugins in the [`server-slim`](../../getting-started/docker-install.md#choose-a-deployment) image, the Deephaven server image that runs Groovy and doesn't include Python, and on a server that doesn't run in Docker. It uses the [Keycloak](https://www.keycloak.org/) authentication plugin as the example. It also explains where the server looks for JS plugins and how to point it at another directory.

## Install JS plugins with Docker

Installing a JS plugin from npm takes a two-stage Docker build. The first stage uses the `web-plugin-packager` image, whose [`pack-plugins.sh`](https://github.com/deephaven/deephaven-core/blob/main/docker/web-plugin-packager/src/main/docker/files/pack-plugins.sh) script downloads the npm packages and writes a `js-plugins/` directory with a `manifest.json` that lists them. The second stage copies that directory into the server image. Use this template for plugins that need no Python package, as described in [Available JS plugins](#available-js-plugins):

```docker title="Dockerfile"
FROM ghcr.io/deephaven/web-plugin-packager:latest AS js-plugins
# 1. Package the npm deephaven-js-plugin(s)
RUN ./pack-plugins.sh <plugins>

FROM ghcr.io/deephaven/server-slim:latest
# 2. Copy the js-plugins/ directory
COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/
```

Replace `<plugins>` with one or more npm package names, separated by spaces. To pin a version, append `@<version>` to a name. To find package names, see [Available JS plugins](#available-js-plugins).

### Example: Keycloak authentication

This Dockerfile installs the [Keycloak](https://www.keycloak.org/) authentication JS plugin.

```docker title="Dockerfile"
FROM ghcr.io/deephaven/web-plugin-packager:latest AS js-plugins
RUN ./pack-plugins.sh @deephaven/js-plugin-auth-keycloak

FROM ghcr.io/deephaven/server-slim:latest
COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/
```

Build and run the image:

```bash
docker build -t my-deephaven-image .
docker run --rm -p 10000:10000 my-deephaven-image
```

To confirm that the server loaded your plugins, open `http://localhost:10000/js-plugins/manifest.json`. It lists each JS plugin the server loaded, by npm package name.

> [!NOTE]
> The Keycloak plugin handles only the client-side authentication UI. Using it end-to-end also requires a running Keycloak server, the Deephaven OIDC authentication provider JAR, and server-side authentication properties. See [Keycloak authentication](../authentication/auth-keycloak.md) for the server-side setup.

For a full walkthrough, including Docker Compose, see [Extend Docker images for JS plugins](../install-use-plugins.md#extend-docker-images-for-js-plugins).

### Plugins that use the loader field

A JS plugin can use exports from another JS plugin by naming it in the [`loader` field](/core/docs/how-to-guides/create-js-plugins/#use-exports-from-another-js-plugin) of its `package.json`. The published `ghcr.io/deephaven/web-plugin-packager:latest` image runs the Deephaven 0.17 version of [`pack-plugins.sh`](https://github.com/deephaven/deephaven-core/blob/main/docker/web-plugin-packager/src/main/docker/files/pack-plugins.sh), which drops that field from `manifest.json`. If a plugin's `package.json` has a `loader` field, build the current packager instead:

1. In a clone of [deephaven-core](https://github.com/deephaven/deephaven-core), run `./gradlew docker-web-plugin-packager:buildDocker`.
2. Use the resulting `deephaven/web-plugin-packager:local-build` image in place of `ghcr.io/deephaven/web-plugin-packager:latest`.

## Install JS plugins on a server outside Docker

If you run the server outside Docker, for example by running the [production application](./configure-production-application.md) directly, you still need a `js-plugins/` directory with a `manifest.json`. You can create one with the packager image. Run this command in an empty directory:

```bash
docker run --rm -v "$PWD:/out" -w /out ghcr.io/deephaven/web-plugin-packager:latest /pack-plugins.sh <plugins>
```

Replace `<plugins>` with one or more npm package names, separated by spaces. To find package names, see [Available JS plugins](#available-js-plugins).

The script writes `js-plugins/` to that directory and uses a temporary `tmp/` subdirectory, which it deletes when it finishes. It fails if `js-plugins/` already exists.

The published packager image doesn't copy a plugin's [`loader` field](/core/docs/how-to-guides/create-js-plugins/#use-exports-from-another-js-plugin) into `manifest.json`, as described in [Plugins that use the loader field](#plugins-that-use-the-loader-field). To package a plugin that has that field, use the `deephaven/web-plugin-packager:local-build` image in place of `ghcr.io/deephaven/web-plugin-packager:latest`, or run [`pack-plugins.sh`](https://github.com/deephaven/deephaven-core/blob/main/docker/web-plugin-packager/src/main/docker/files/pack-plugins.sh) from the `main` branch as described next.

To run [`pack-plugins.sh`](https://github.com/deephaven/deephaven-core/blob/main/docker/web-plugin-packager/src/main/docker/files/pack-plugins.sh) without Docker, you need bash, Node.js, npm 7.20 or later, and GNU `tar` and `rm`. Run it as `./pack-plugins.sh <plugins>`. It writes the same `js-plugins/` directory to the current directory, so run it in an empty directory.

Copy the `js-plugins/` directory into the server's configuration directory, as described in [Where the server loads JS plugins](#where-the-server-loads-js-plugins), and restart the server.

## Where the server loads JS plugins

By default, the server loads JS plugins from `<configDir>/js-plugins/`, where `<configDir>` is the server's configuration directory. The Deephaven Docker images set the configuration directory to `/opt/deephaven/config/` (through `DEEPHAVEN_CONFIG_DIR`), so the Dockerfile in [Install JS plugins with Docker](#install-js-plugins-with-docker) copies the plugins to `/opt/deephaven/config/js-plugins/`. For other setups, see [Deephaven server bootstrap configuration](./configure-production-application.md#deephaven-server-bootstrap-configuration).

The server discovers plugins through a `manifest.json` file in `<configDir>/js-plugins/`, which [`pack-plugins.sh`](https://github.com/deephaven/deephaven-core/blob/main/docker/web-plugin-packager/src/main/docker/files/pack-plugins.sh) generates:

- The server loads only the plugins that `manifest.json` lists.
- If the directory has no `manifest.json`, the server loads no plugins from it.

To load JS plugins from another directory as well, set the [configuration property](./config-file.md) `deephaven.jsPlugins.resourceBase` to it. The server then loads plugins from both directories.

This directory must contain a `manifest.json`. If it doesn't, the server fails to start.

If you're developing a plugin, you can instead point a `deephaven.jsPlugins.<name>` property at a single npm package directory. See [Register an npm package directory during development](../create-js-plugins.md#register-an-npm-package-directory-during-development).

## Available JS plugins

Deephaven maintains a set of JS plugins in the [deephaven/deephaven-plugins](https://github.com/deephaven/deephaven-plugins) GitHub repository. For a plugin with a published JS component, the npm package name is the `name` field of `src/js/package.json` in that plugin's folder of the repository.

Most of these plugins have the keyword `deephaven-js-plugin` on [npm](https://www.npmjs.com/), so you can [search npm for that keyword](https://www.npmjs.com/search?q=keywords%3Adeephaven-js-plugin) to find them.

On [`server-slim`](../../getting-started/docker-install.md#choose-a-deployment), choose plugins that need no Python package, such as [`@deephaven/js-plugin-theme-pack`](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/theme-pack) or `@deephaven/js-plugin-auth-keycloak`. A Groovy server can't run a plugin's Python package, so a plugin that needs one has no server-side component for its JS to work with.

To suggest a new plugin for Deephaven to develop, [open an issue in deephaven-plugins](https://github.com/deephaven/deephaven-plugins/issues). To build your own, see [Create a JavaScript plugin](../create-js-plugins.md).

## Related documentation

- [Install and use plugins](../install-use-plugins.md)
- [Install and run Deephaven with Docker](../../getting-started/docker-install.md)
- [Create a JavaScript plugin](../create-js-plugins.md)
- [Create a Deephaven configuration file](./config-file.md)
- [Configure the Deephaven Docker application](./docker-application.md)
- [Configure the Deephaven production application](./configure-production-application.md)
- [Configure Keycloak for authentication](../authentication/auth-keycloak.md)
