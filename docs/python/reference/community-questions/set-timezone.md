---
title: How do I set the default time zone in Deephaven Community Core?
---

Deephaven has two separate time zone settings: the time zone the UI uses to display date-time values, and the default time zone the engine uses for calculations.

## Change the displayed time zone in the UI

Setting the time zone in the UI changes how date-time data gets displayed, but not how it's handled by the engine. Open the **Settings** menu (the gear icon) at the top right of the UI, and choose a time zone from the **Time Zone** dropdown. For more information, see [How to set date-time format](../../how-to-guides/set-date-time-format.md).

## Change the default time zone used by the engine

To change the default time zone used by the engine, set the JVM's `user.timezone` system property (for example, `-Duser.timezone=America/New_York`) when you start Deephaven.

The following Docker Compose file modifies Deephaven's [basic Python configuration](https://github.com/deephaven/deephaven-core/blob/main/containers/python/base/docker-compose.yml) by setting the default time zone to `America/New_York`:

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -Duser.timezone=America/New_York
```

The following Docker command modifies the one-liner in our [quickstart](../../getting-started/quickstart.md#1-install-and-launch-deephaven) to set the default time zone to `America/New_York`:

```sh
docker run --rm --name deephaven -p 10000:10000 -v "$(pwd)/data:/data" --env START_OPTS="-Dauthentication.psk=YOUR_PASSWORD_HERE -Duser.timezone=America/New_York" ghcr.io/deephaven/server:latest
```

The following Python code starts a Deephaven server [from Python](../../getting-started/pip-install.md) with the default time zone set to `America/New_York`:

```python skip-test
from deephaven_server import Server

s = Server(port=10000, jvm_args=["-Xmx4g", "-Duser.timezone=America/New_York"])
s.start()
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
