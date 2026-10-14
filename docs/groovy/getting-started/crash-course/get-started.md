---
title: Get Started
---

_A Crash Course in Deephaven_ is your backpack guide through the world of real-time data analysis using the Deephaven data engine.

This first chapter gets a Deephaven server running and opens the Deephaven IDE, where you run the code for the rest of the crash course.

## Start Deephaven

To follow along, ensure you have [Docker](https://docs.docker.com/engine/install/) installed on your machine.

Once Docker is installed, run this command:

```bash skip-test
docker run --rm --name deephaven -p 10000:10000 -v "$(pwd)/data:/data" --env START_OPTS=-Dauthentication.psk=YOUR_PASSWORD_HERE ghcr.io/deephaven/server-slim:latest
```

> [!CAUTION]
> Replace `YOUR_PASSWORD_HERE` with a secure password of your own to keep your session safe. Deephaven uses this password as its [pre-shared key](../../how-to-guides/authentication/auth-psk.md) to authenticate you.

The server keeps running in this terminal and prints its log there. When the log shows `Server started on port 10000`, the server is ready.

The command's options do the following:

- `-p 10000:10000` makes the container's port 10000 available on your machine, so you can open the IDE at `http://localhost:10000/ide/` in [Open the IDE](#open-the-ide).
- `--env START_OPTS=...` passes your password to the server as a startup option. The [Configure your Deephaven instance](./configure.md) chapter near the end of this crash course uses `START_OPTS` to set other options, such as the server's maximum Java heap size.
- `-v "$(pwd)/data:/data"` mounts a `data` directory from your current working directory at `/data` in the container. Docker creates the directory if it doesn't exist.
- `--rm` deletes the container when it stops.
- `--name deephaven` names the container `deephaven`, so you can refer to it by name in other Docker commands, such as `docker stop deephaven`.

Files you save in the IDE go to `data/storage` on your machine. Anything your code writes under `/data` in the container also appears in your local `data` directory. Because these files live on your machine, you keep your work after `--rm` deletes the container.

## Get the example data

The [Data I/O](./export-data.md) chapter reads Deephaven's example datasets from `/data/examples` in the container. To download the [Deephaven examples](https://github.com/deephaven/examples) into your local `data/examples` directory, open a second terminal in the same directory and run:

```bash skip-test
docker run --rm -v "$(pwd)/data:/data" ghcr.io/deephaven/examples download
```

## Open the IDE

Open the Deephaven IDE at `http://localhost:10000/ide/`, enter your password in the **Token** field, and you're ready to go!

Once you're in, the **Console** on the left is where you write and run code. Results, such as tables and plots, open as panels in the workspace. The **Panels** menu at the top right of the IDE lists the tables, plots, and other objects in your session. Select one to open it. See [Navigate the GUI](../../how-to-guides/user-interface/navigating-the-ui.md) for a full tour of the interface.

Next, [Architecture overview](./architecture-overview.md) explains how Deephaven's tables, engine, and user interface fit together.
