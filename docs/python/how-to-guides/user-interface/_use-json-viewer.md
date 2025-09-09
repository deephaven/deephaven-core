---
title: Use the Deephaven JSON viewer
sidebar_label: JSON Viewer
---

Deephaven's UI can be used to display JSON objects. This guide shows how to build Deephaven with the [NPM](https://www.npmjs.com/package/@deephaven/js-plugin-dashboard-object-viewer) and [PyPi](https://pypi.org/project/deephaven-plugin-json/) JSON viewer packages, and how to use them in the UI.

## Docker images

In order to display JSON objects in the Deephaven UI, you need to build Deephaven with custom dependencies. If you're unfamiliar on how to do this, check out the [How to install packages](../install-packages.md) guide.

We need to install dependencies on both the `server` and `web` images. Since we're building two Docker images, we'll put our `Dockerfile`s in separate directories: `./server/Dockerfile` and `./web/Dockerfile`.

### ./server/Dockerfile

Put the following in `./server/Dockerfile`:

```
FROM ghcr.io/deephaven/server:${VERSION:-latest}

RUN pip install deephaven-plugin-json
```

This allows us to use the Python package in the console.

### ./web/Dockerfile

Put the following in `./web/Dockerfile`:

```
FROM ghcr.io/deephaven/web-plugin-packager:main as build

RUN ./pack-plugins.sh @deephaven/js-plugin-dashboard-object-viewer

FROM ghcr.io/deephaven/web:${VERSION:-latest}
COPY --from=build js-plugins/ /usr/share/nginx/html/js-plugins/
```

This adds the object viewer plugin to the Deephaven UI.

### docker-compose.yml

Next, we need to update the `docker-compose.yml` file to point to these images.

#### Local build

One option is to follow the `docker build <path>` pattern described in the [How to install packages](../install-packages.md) guide. We can run the following commands to build our Docker images.

```
docker build ./server --tag deephaven/custom-server
docker build ./web --tag deephaven/custom-web
```

Then, we update our `docker-compose.yml` file to point to these images.

```
services:
  server:
    image: deephaven/custom-server
...
  web:
    image: deephaven/custom-web
```

#### docker-compose.yml `build` field

The other option is to remove the `image` field in the `docker-compose.yml` file and replace it with `build`. `build` would point to the directory of the `Dockerfile`.

```
services:
  server:
    build: ./server
...
  web:
    build: ./web
```

Once this is done, we can run `docker compose build` to build the images.

## Launch

Once we've set up our Docker images, we can simply run `docker compose up` to launch Deephaven.

In the console, run the following query to use the JSON plugin and view JSON objects in the UI.

```python skip-test
from deephaven.TableTools import emptyTable
from deephaven.plugin.json import Node

t = emptyTable(100).update("X=i")
node_a = Node({"a": "hello, world"})
node_b = Node({"b": "goodbye", "t": t})
```
