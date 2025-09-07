---
title: Get Started
sidebar_label: Get Started
---

_A Crash Course in Deephaven_ is your backpack guide through the world of real-time data analysis using the Deephaven data engine. This guide provides a broad - but clear and technically informative - overview of Deephaven’s capabilities. Let's dive in and unlock the potential of this powerful platform.

To follow along, ensure you have [Docker](https://docs.docker.com/engine/install/) installed on your machine.

Once [Docker](https://docs.docker.com/engine/install/) is installed, execute this [Docker command](../../tutorials/docker-install.md#start-the-application):

```bash skip-test
docker run --rm --name deephaven -p 10000:10000 -v data:/data --env START_OPTS=-Dauthentication.psk=YOUR_PASSWORD_HERE ghcr.io/deephaven/server-slim:latest
```

> [!CAUTION]
> Replace "YOUR_PASSWORD_HERE" with a more secure passkey to keep your session safe.

Open the Deephaven IDE at `http://localhost:10000/ide/`, enter your password in the password field, and you're ready to go!

The Docker command above creates a directory in your local working directory called `data`. Any files that you save in Deephaven will be stored there, ensuring you won't lose any valuable work. To learn more about mounting directories in Docker, check out [this guide](../../conceptual/docker-data-volumes.md).

Deephaven can be configured and run in many different ways. More information on configuration options are discussed at the [end of this crash course](./configure.md).
