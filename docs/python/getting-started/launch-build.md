---
title: Build and run Deephaven from source code
sidebar_label: Source code
---

This guide will show you how to build and launch Deephaven Community Core from source code.
It provides a starting point for tinkerers and developers who want to dig into configuration or
experiment with code changes. These instructions show how to build on multiple operating
systems, including Linux, Windows, and Mac.

> [!TIP]
> Launching from source code is recommended for users who wish to tinker with and modify source code. For an easier installation method, see [Launch Deephaven from pre-built images](./docker-install.md).

## Supported operating systems

Deephaven is only supported on:

- Linux
- MacOS
- Windows 10 or 11 (requires [WSL 2 (Windows Subsystem for Linux v2)](https://learn.microsoft.com/en-us/windows/wsl/install))

## Prerequisites

Building and running a Deephaven Python server from source code requires a couple of software packages.

### Java

Your Java installation must be version **17** or later.

Deephaven requires a JDK (Java Development Kit), not just a JRE (Java Runtime Environment). The JDK includes the Java compiler and other tools needed for building and running Java applications.

You can check your Java version with:

```bash
java --version
```

> [!NOTE]
> The Java build process uses [Gradle Auto Provisioning](https://docs.gradle.org/current/userguide/toolchains.html#sec:provisioning) to download and use the appropriate Java version for building and testing.

### Python

It is recommended you stay up-to-date with the latest Python version. Deephaven requires Python **3.9** or later. You can check your Python version with:

```bash
python --version
```

### Version control

Deephaven highly recommends using a version control system to clone the [deephaven-core repository](https://github.com/deephaven/deephaven-core). The most popular and common option is [git](https://git-scm.com/); this guide uses it to clone the repository.

You can download a ZIP file of the repository from GitHub. However, this is not recommended, as it will be more difficult to stay up-to-date with the latest changes. Additionally, certain files in the repository are managed by [git-lfs](https://git-lfs.com/), which are not included in the ZIP file.

## Build and run Deephaven

The following instructions are a condensed version of instructions found in the [deephaven-core repository](https://github.com/deephaven/deephaven-core). For the full instructions with explanations of configuration parameters, SSL, and more, see the [README](https://github.com/deephaven/deephaven-core/blob/main/server/jetty-app/README.md).

### Clone the deephaven-core repository

Once all of the required dependencies are installed and functioning, clone [https://github.com/deephaven/deephaven-core](https://github.com/deephaven/deephaven-core). If you use `git`, clone it like this:

```bash
git clone https://github.com/deephaven/deephaven-core.git
```

Then, `cd` into your cloned repository:

```bash
cd deephaven-core
```

### Set up the Python virtual environment

First, set up a virtual environment.

```bash
python -m venv /tmp/my-dh-venv
source /tmp/my-dh-venv/bin/activate
```

### Build and install the wheel

Then, build and install the wheel.

```bash
./gradlew py-server:assemble

pip install --find-links py/server/build/wheel "deephaven-core[autocomplete]"
```

### Build and run

Lastly, build and run Deephaven.

```bash
./gradlew server-jetty-app:run
```

## Run Deephaven IDE

Once Deephaven is running, you can launch a Deephaven IDE in your web browser. Deephaven IDE allows you to interactively analyze data and develop new analytics.

- If Deephaven is running locally, navigate to [http://localhost:10000/ide/](http://localhost:10000/ide/).
- If Deephaven is running remotely, navigate to `http://<hostname>:10000/ide/`, where `<hostname>` is the address of the machine Deephaven is running on.

### Authentication

Deephaven, by default, uses [pre-shared key authentication](../how-to-guides/authentication/auth-psk.md). If no key is set, a randomly generated key will be used to log into the server each time it starts. The randomly generated key is printed to the Docker logs like this:

![Log readout with randomly generated PSK](../assets/tutorials/default-psk.png)

To set your own pre-shared key, add `-Ppsk=<YourPasswordHere>`:

```bash
./gradlew server-jetty-app:run -Ppsk=YOUR_PASSWORD_HERE
```

The pre-shared key is printed to the Docker log like this:

![Log readout with user-defined PSK](../assets/how-to/custom-psk2.png)

## Related documentation

- [Create a new table](../how-to-guides/new-and-empty-table.md#new_table)
- [Joins: Exact and Relational](../how-to-guides/joins-exact-relational.md)
- [Joins: Time-Series and Range](../how-to-guides/joins-timeseries-range.md)
