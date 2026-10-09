---
title: Build and run Deephaven from source code
---

This guide shows you how to build and launch Deephaven Community Core from source code. It provides a starting point for tinkerers and developers who want to dig into configuration or experiment with code changes.

> [!TIP]
> For an easier installation method, see [Install and run with Docker](./docker-install.md) or [Install and run Deephaven with pip](./pip-install.md).

> [!NOTE]
> This guide builds and runs Deephaven with Python. For Groovy, see [Build and run Deephaven from source code (Groovy)](/core/groovy/docs/getting-started/launch-build).

## Supported operating systems

You can build and run Deephaven from source only on the following operating systems:

- Linux
- macOS
- Windows 10 build 20262 or higher, or Windows 11, through [Windows Subsystem for Linux 2 (WSL 2)](https://learn.microsoft.com/en-us/windows/wsl/install)

On Windows, run every command in this guide inside a WSL 2 terminal.

> [!WARNING]
> WSL 2's default time-sync setup can cause spurious 10–20-second clock jumps that stall Deephaven [ticking tables](./crash-course/create-tables.md#ticking-tables), which update live as new data arrives. Before running Deephaven on WSL 2, apply one of the [time-sync workarounds](../reference/community-questions/wsl2-clock-drift.md).

## Prerequisites

Before you build Deephaven from source, install the following software. Deephaven builds with [Gradle](https://gradle.org/), but you don't need to install it. The repository includes the [Gradle Wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html), a `gradlew` script that downloads and runs the correct version of Gradle automatically.

### Java

You must install a JDK (Java Development Kit) version **21**, not just a JRE (Java Runtime Environment). The JDK includes the Java compiler and other tools the build needs. Your JDK 21 runs the Gradle build, and Gradle does not download it for you.

To compile, test, and run Deephaven, Gradle uses [toolchain auto-provisioning](https://docs.gradle.org/current/userguide/toolchains.html#sec:provisioning) to download any other Java version it needs, so JDK 21 is the only Java version you install.

You can check that a JDK 21 is installed with:

```bash
javac --version
```

### Docker

Building Deephaven from source requires [Docker](https://docs.docker.com/get-docker/) version 20.10.8 or later. The Gradle build runs some steps in Docker, such as generating the server's gRPC code, packaging the web UI, and building the Python server package. Start the Docker daemon before you run the `./gradlew` commands in this guide. On Windows, enable Docker's WSL 2 integration.

You can check your Docker version with:

```bash
docker version
```

### Python

Deephaven requires Python **3.9** or later. See the [version matrix](../reference/version-matrix.md#python-versions) for the Python versions each release supports. You can check your Python version with:

```bash
python3 --version
```

### Version control

We recommend using a version control system to clone the [deephaven-core repository](https://github.com/deephaven/deephaven-core). The most popular option is [Git](https://git-scm.com/), and this guide uses it to clone the repository.

You can download a ZIP file of the repository from GitHub instead, but we don't recommend it, because a ZIP download is harder to keep up to date.

## Build and run Deephaven

The following steps condense the build instructions in the [deephaven-core repository](https://github.com/deephaven/deephaven-core). For the full instructions with explanations of configuration parameters, SSL, and more, see the [Jetty server README](https://github.com/deephaven/deephaven-core/blob/main/server/jetty-app/README.md).

### Clone the deephaven-core repository

Once you've installed the [prerequisites](#prerequisites), clone the deephaven-core repository with Git:

```bash
git clone https://github.com/deephaven/deephaven-core.git
```

Then, `cd` into your cloned repository:

```bash
cd deephaven-core
```

You can verify the Gradle Wrapper is present:

```bash
ls gradlew
```

### Set up the Python virtual environment

Next, from the repository root, create and activate a Python virtual environment.

```bash
python3 -m venv ~/my-dh-venv
source ~/my-dh-venv/bin/activate
```

### Build and install the wheel

Then, build the Deephaven server's Python package, called a wheel, and install it into the virtual environment. The `[autocomplete]` suffix installs optional dependencies that add code autocompletion in the IDE.

```bash
./gradlew py-server:assemble

pip install --find-links py/server/build/wheel "deephaven-core[autocomplete]"
```

### Start the server

Lastly, start the Deephaven server from a shell where the virtual environment is active. The server runs Python from that environment, so in a new shell, run `source ~/my-dh-venv/bin/activate` first.

```bash
./gradlew server-jetty-app:run
```

The first build can take several minutes. The command keeps running in the foreground for as long as the server is up. When the log shows `Server started on port 10000`, the server is ready. To stop it, press <kbd>Ctrl</kbd> + <kbd>C</kbd> in the terminal where the server is running.

## Open the Deephaven IDE

Once Deephaven is running, open the Deephaven IDE in your web browser. The IDE lets you analyze data interactively and develop new analytics.

- If Deephaven is running locally, navigate to [http://localhost:10000/ide/](http://localhost:10000/ide/).
- If Deephaven is running remotely, navigate to `http://<hostname>:10000/ide/`, where `<hostname>` is the address of the machine Deephaven is running on.

The IDE asks for a pre-shared key before it opens. To skip this prompt, open the URL that the server log prints after `Connect automatically to Web UI with`, which already includes the key. See [Authentication](#authentication) to find the key and the URL.

### Authentication

By default, Deephaven uses [pre-shared key authentication](../how-to-guides/authentication/auth-psk.md). If you don't set a key, Deephaven generates a random key each time it starts and prints it to the server log in the terminal where you ran `./gradlew server-jetty-app:run`, like this:

![Log readout with randomly generated PSK](../assets/tutorials/default-psk.png)

To set your own pre-shared key, stop the server and start it again with `-Ppsk=YOUR_PASSWORD_HERE`:

```bash
./gradlew server-jetty-app:run -Ppsk=YOUR_PASSWORD_HERE
```

Deephaven prints your key to the server log like this:

![Log readout with user-defined PSK](../assets/how-to/custom-psk2.png)

## Rebuild after changing Python code

If you change the Python server code, stop the server, then rebuild the wheel and reinstall it:

```bash
./gradlew py-server:assemble
pip install --force-reinstall --no-deps --find-links py/server/build/wheel deephaven-core
```

The rebuilt wheel has the same version number as the installed one. Without `--force-reinstall`, `pip` treats the package as already installed and skips it.

Then start the server again with `./gradlew server-jetty-app:run`.

If you change Python code often, see the editable-install instructions in the [Python development README](https://github.com/deephaven/deephaven-core/blob/main/py/README.md).

If you change only Java code, you don't need to rebuild the wheel. Stop the server and run `./gradlew server-jetty-app:run` again. Gradle recompiles the changed code before it starts the server.

## Related documentation

- [Pre-shared key authentication](../how-to-guides/authentication/auth-psk.md)
- [Install and run with Docker](./docker-install.md)
- [Install and run Deephaven with pip](./pip-install.md)
- [Install and run the Deephaven production application](./production-application.md)
- [Quickstart](./quickstart.md)
