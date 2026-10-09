---
title: Get Started
---

_A Crash Course in Deephaven_ is your backpack guide through the world of real-time data analysis using the Deephaven data engine. This guide provides a broad, clear, and technically informative overview of Deephaven's capabilities. Let's dive in and unlock the potential of this powerful platform.

To follow along, ensure you have [Docker](https://docs.docker.com/engine/install/) installed on your machine. If you prefer to use [pip](https://packaging.python.org/en/latest/guides/tool-recommendations/) to install Deephaven, check out the [guide on installing Deephaven with pip](../../getting-started/pip-install.md).

Once [Docker](https://docs.docker.com/engine/install/) is installed, execute this [Docker command](../../getting-started/docker-install.md#the-simplest-possible-installation):

```bash skip-test
docker run --rm --name deephaven -p 10000:10000 -v "$(pwd)/data:/data" --env START_OPTS=-Dauthentication.psk=YOUR_PASSWORD_HERE ghcr.io/deephaven/server:latest
```

> [!CAUTION]
> Replace "YOUR_PASSWORD_HERE" with a more secure passkey to keep your session safe.

Open the Deephaven IDE at `http://localhost:10000/ide/`, enter your password in the password field, and you're ready to go!

Once you're in, the **Console** on the left is where you write and run code. Results, such as tables and plots, open as panels in the workspace. The **Tables** and **Widgets** dropdowns at the top list the objects in your session. See [Navigate the GUI](../../how-to-guides/user-interface/navigating-the-ui.md) for a full tour of the interface.

The Docker command above mounts a `data` directory in your local working directory into the container at `/data`; Docker creates it if it does not exist. Files you save in the Deephaven IDE (under `data/storage`) and any other files written to `/data` in the container are stored there, so you won't lose your work when the container stops. To learn more about mounting directories in Docker, see [Docker data volumes](../../conceptual/docker-data-volumes.md).

Deephaven can be configured and run in many different ways. More information on configuration options appears at the [end of this crash course](./configure.md).
