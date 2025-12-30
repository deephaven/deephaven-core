---
title: How do I set the default timezone in Deephaven Community Core?
sidebar_label: How do I set the default timezone?
---

There are two ways to set the default time zone in Deephaven.

## Change displayed time zone in the UI

Setting the default time zone via the UI changes how date-time data gets displayed, but not how it's handled by the engine. Click the gear icon in the top right corner of the UI, and set the timezone via the dropdown menu. For more information, see [here](../../how-to-guides/set-date-time-format.md).

## Change the default time zone used by the engine

To actually change the default time zone used by the engine, you need to pass an additional space-separated configuration property when you start Deephaven.

The following docker-compose modifies Deephaven's [basic Python configuration](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/base/docker-compose.yml) by setting the default timezone to `America/New_York`:

```sh
version: "3.4"

services:
  deephaven:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -Duser.timezone="America/New_York"
```

The following Docker command modifies the one-liner in our [quickstart](../../getting-started/quickstart.md#1-install-and-launch-deephaven) to set the default timezone to `America/New_York`:

```sh
docker run --rm --name deephaven -p 10000:10000 --env START_OPTS=-Dauthentication.psk=YOUR_PASSWORD_HERE -Duser.timezone="America/New_York" ghcr.io/deephaven/server:latest
```

The following Python code starts a Deephaven server [from Python](../../getting-started/pip-install.md) with the default timezone set to `America/New_York`:

```python skip-test
from deephaven_server import Server

s = Server(port=10000, jvm_args=["-Xmx4g", "-Duser.timezone='America/New_York'"])
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
