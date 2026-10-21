---
title: Retrieve logs
---

As with any software, sometimes things go wrong: a query fails, the server crashes, or the web UI misbehaves. When that happens, the logs record what went wrong, so you can find the root cause. This guide shows you how to retrieve Deephaven's logs and share them when you ask for help.

Deephaven produces two kinds of logs:

- **Server logs**: read them in the [Log panel](#log-panel) or the [server container's output](#retrieve-docker-container-logs), copy out [JVM crash files](#retrieve-a-jvm-crash-file), and [enable debug logging](#enable-debug-logging) for more detail.
- **Web UI logs**: review the [Command History panel](#command-history), [export support logs](#export-support-logs), and read the [browser console](#view-the-browser-console).

## Server logs

The commands in this section use [Docker](../getting-started/docker-install.md). If you [build and run Deephaven from source](../getting-started/launch-build.md) with Gradle, the server prints the same log to the terminal where you started it.

### Log panel

The **Log** panel in the web UI shows the server's log as it runs, without the need for a terminal or Docker command. When you open it, the panel shows the most recent messages the server has kept in memory and then adds new ones as they arrive. Older messages drop off, so open the panel soon after the problem occurs.

To open the panel, click the **Log** tab next to the **Console** tab. [Navigate the GUI](./user-interface/navigating-the-ui.md) shows where the tab sits in the default layout.

By default, the panel shows only `STDOUT`, `STDERR`, `WARN`, `ERROR`, and `FATAL` messages. To see `INFO` or `DEBUG` messages too, click the gear (**Log Settings**) button at the top right of the Log panel and select them under **Display Log Levels**.

The panel needs a running server and holds only recent messages. When the server has stopped, or you need the full log, use [`docker logs`](#retrieve-docker-container-logs).

### Retrieve Docker container logs

If you start Deephaven in detached mode, with [`docker compose up -d`](https://docs.docker.com/reference/cli/docker/compose/up/) or [`docker run -d`](https://docs.docker.com/reference/cli/docker/container/run/), the server's output doesn't appear in your terminal. To see it, use the [`docker logs`](https://docs.docker.com/reference/cli/docker/container/logs/) command. The command works for any container, whether it runs detached or in the foreground, and whether it's running or stopped.

Replace `<CONTAINER>` with the name of your server container:

```shell
docker logs <CONTAINER>
```

> [!NOTE]
> If you don't know the name of your container, run [`docker ps -a`](https://docs.docker.com/reference/cli/docker/container/ls/). It lists all containers, including stopped ones:
>
> ```shell
> docker ps -a
> ```

Redirect the logs to a file so you can search them. The server prints its log to standard output, and the JVM prints some startup messages to standard error, so redirect both:

```shell
docker logs <CONTAINER> > log.txt 2>&1
```

If you start a container with `--rm`, as the [Quickstart](../getting-started/quickstart.md) does, Docker removes it as soon as it stops. The container's logs and any crash file go with it. To keep them after the server stops, start the container without `--rm`.

### Retrieve a JVM crash file

When the JVM itself crashes, for example with a segmentation fault (a `SIGSEGV` signal):

- The server process stops.
- The JVM writes a crash report file named `hs_err_pid<PID>.log`, where `<PID>` is the JVM's process ID.
- The server log gives the file's full path on the line after `# An error report file with more information is saved as:`.

In the Deephaven Docker images, the JVM writes the file to `/hs_err_pid1.log`. If you run the container with an init process ([`docker run --init`](https://docs.docker.com/reference/cli/docker/container/run/) or `init: true` in Docker Compose), the process ID differs, so use the path from the server log.

Use [`docker cp`](https://docs.docker.com/reference/cli/docker/container/cp/) to copy the file to your local machine. The container doesn't need to be running, but it must still exist, so copy the file before you run [`docker compose down`](https://docs.docker.com/reference/cli/docker/compose/down/) or [`docker rm`](https://docs.docker.com/reference/cli/docker/container/rm/).

If you started the container with `--rm`, the crash file is already gone; see [Retrieve Docker container logs](#retrieve-docker-container-logs).

Replace `<CONTAINER>` with the name of your server container, and change the path if the server log gives a different one:

```shell
docker cp <CONTAINER>:/hs_err_pid1.log ./
```

### Enable debug logging

You can fine-tune how much detail the server logs. By default, the server logs messages at the `INFO` level and above. For more detail, switch to the `logback-debug.xml` configuration file for Logback, the server's logging framework. This file logs at the `DEBUG` level.

To turn it on, add `-Dlogback.configurationFile=logback-debug.xml` to the `START_OPTS` environment variable, which passes JVM options to the server. For more about `START_OPTS`, see [Modify the deployment](../getting-started/docker-install.md#modify-the-deployment).

Both Docker methods below create a new server container, which deletes the old container's logs and any crash file in it. If the server crashed, first [save its logs](#retrieve-docker-container-logs) and [copy its crash file](#retrieve-a-jvm-crash-file).

In `docker-compose.yml`, add the option to the existing `START_OPTS` line:

```yaml
services:
  deephaven:
    environment:
      - START_OPTS=-Xmx4g -Dlogback.configurationFile=logback-debug.xml
```

Then run [`docker compose up -d`](https://docs.docker.com/reference/cli/docker/compose/up/) from the directory that holds `docker-compose.yml`. Docker Compose recreates the container with the new setting.

Without Docker Compose, pass the option to [`docker create`](https://docs.docker.com/reference/cli/docker/container/create/) with `--env`:

1. Remove your existing server container with [`docker rm -f <CONTAINER>`](https://docs.docker.com/reference/cli/docker/container/rm/).
2. Create and start a new container named `deephaven` with the commands below. Add every option you used for the original container, such as `-v` volume mounts and other `START_OPTS` values like `-Dauthentication.psk`. Otherwise, the new container starts without them.

```shell
docker create --name deephaven -p 10000:10000 --env START_OPTS="-Xmx4g -Dlogback.configurationFile=logback-debug.xml" ghcr.io/deephaven/server-slim:latest
docker start -a deephaven
```

If you [build and run Deephaven from source](../getting-started/launch-build.md), set the variable when you start the server:

```shell
START_OPTS="-Dlogback.configurationFile=logback-debug.xml" ./gradlew server-jetty-app:run -Pgroovy
```

The extra `DEBUG` messages appear in the same server log, so retrieve them with [`docker logs`](#retrieve-docker-container-logs), or read them in your terminal if you built from source. To see them in the [Log panel](#log-panel), select **DEBUG** under **Display Log Levels**.

## Web UI logs

### Command history

Deephaven keeps a record of the commands you've run in the console, how long each one took, and whether it produced an error. The **Command History** panel shows this information, and you can search it. [Navigate the GUI](./user-interface/navigating-the-ui.md) shows where the panel sits in the default layout.

![The Command History panel](../assets/how-to/logs1groovy.png)

Hover over a command to see its details.

The command in the image below was successful and took 1.24 seconds.

![A successful command's details in the Command History panel](../assets/how-to/logs2groovy.png)

The command in the next image, however, resulted in an error after three seconds.

![A failed command's details in the Command History panel](../assets/how-to/logs3.png)

The error message gives an ID. Search the [server log](#retrieve-docker-container-logs) for that ID to find the full error.

### Export support logs

To share your logs with other Deephaven users on [GitHub Discussions or Slack](#get-more-help), open **User Settings** and click **Export Logs**.

![The User Settings icon](../assets/how-to/user_settings.png)

![The Export Logs button in the User Settings menu](../assets/how-to/support_settings.png)

**Export Logs** downloads a zip file that holds the web UI's browser logs and the current state of the application. It doesn't include the server log, so attach that separately with [`docker logs`](#retrieve-docker-container-logs).

### View the browser console

If the Deephaven web UI misbehaves, your browser's console logs can help you diagnose the problem. For most reports, [Export Logs](#export-support-logs) is enough. Use the browser console when you need the raw console output or extra detail.

To access browser logs in Chrome, click the three-dot menu at the top right of the browser window and choose **More tools > Developer tools**. This opens the Chrome DevTools panel.

![The More tools > Developer tools option in Chrome's three-dot menu](../assets/how-to/browser1.png)

Click the **Console** tab. The browser logs messages here, including errors and warnings.

![Chrome's Console tab](../assets/how-to/browser2.png)

In the **Default levels** drop-down next to the **Filter** field, select every level: **Verbose**, **Info**, **Warnings**, and **Errors**. We recommend this so that no message is hidden:

![All levels selected next to the Filter field](../assets/how-to/browser3.png)

To capture more detailed information, change the DevTools console settings. In DevTools, click **Settings** (the gear icon) in the top action bar, or press F1:

![The DevTools Settings button](../assets/how-to/browser4.png)

Under **Console**, you can choose options such as **Log XMLHttpRequests** and **Show timestamps**:

![The Console options, with boxes to tick if you want to log XMLHttpRequests, show timestamps, or more](../assets/how-to/browser5.png)

To share the raw console output, right-click within the **Console** and choose **Save as**. The saved file includes the extra messages from **Log XMLHttpRequests**.

![The Console's right-click menu, with the Save as option highlighted](../assets/how-to/browser6.png)

## Get more help

Ask questions in our [GitHub Discussions](https://github.com/deephaven/deephaven-core/discussions/categories/q-a) forum or in our [Community Slack](/slack). Attach the logs you collected with the steps above to help others find the cause.

## Related documentation

- [Triage errors in queries](./triage-errors.md)
- [Decipher Groovy errors](./groovy-errors.md)
- [Get version information](./get-version.md)
- [Quickstart](../getting-started/quickstart.md)
- [Install and run with Docker](../getting-started/docker-install.md)
- [Build and run Deephaven from source code](../getting-started/launch-build.md)
- [Navigate the GUI](./user-interface/navigating-the-ui.md)
