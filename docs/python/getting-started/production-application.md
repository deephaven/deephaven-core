---
title: Install and run the Deephaven production application
---

This guide shows you how to install and launch the Deephaven Community Core production application. The production application runs Deephaven from artifacts built for each release, so you don't need Docker or a source build. It is the recommended way to run production applications that use Deephaven, hence its name.

This guide covers installing and starting the server, setting authentication, and running more than one server on a machine. For more advanced options, see [Configure the production application](../how-to-guides/configuration/configure-production-application.md).

## Supported operating systems

Deephaven runs on these operating systems. See the [version matrix](../reference/version-matrix.md) for which platforms Deephaven tests regularly and which it only expects to work.

- Linux
- macOS
- Windows 10 or Windows 11, with [WSL 2](https://learn.microsoft.com/en-us/windows/wsl/install).

> [!WARNING]
> WSL 2's default time-sync setup can cause spurious 10–20-second clock jumps that stall Deephaven [ticking tables](../conceptual/table-types.md). Before running Deephaven on WSL 2, apply one of the [time-sync workarounds](../reference/community-questions/wsl2-clock-drift.md).

## Prerequisites

[Java](https://www.java.com/en/) and [Python](https://www.python.org/) are the only prerequisites to run Deephaven's production application.

| Name   | Recommended version | Required version   |
| ------ | ------------------- | ------------------ |
| Java   | 21 (LTS)            | >= 17              |
| Python | 3.12                | See version matrix |

The minimum Python version depends on the release. See the [version matrix](../reference/version-matrix.md) for the Python and Java versions each release supports.

## Get the artifacts

The Deephaven artifacts are attached to each [Deephaven release](https://github.com/deephaven/deephaven-core/releases). Only the server artifact in tar format is required. To install the Python package from a wheel instead of PyPI, also download the Python artifact in wheel format.

Set your preferred version in an environment variable. The shell commands in this guide set and use `DH_VERSION`. Replace `LATEST_VERSION_HERE` with the version number of the [most recent release](https://github.com/deephaven/deephaven-core/releases/latest), without the leading `v`:

```bash
export DH_VERSION=LATEST_VERSION_HERE
```

You can download release artifacts from your browser of choice or via the command line:

```bash
wget https://github.com/deephaven/deephaven-core/releases/download/v${DH_VERSION}/server-jetty-${DH_VERSION}.tar
# Optional: only needed to install the Python wheel
wget https://github.com/deephaven/deephaven-core/releases/download/v${DH_VERSION}/deephaven_core-${DH_VERSION}-py3-none-any.whl
```

## Unpack the server

Unpack the server artifact from your file explorer or with the command line:

```bash
tar xvf server-jetty-${DH_VERSION}.tar
```

## Create the virtual environment

We highly recommend a [virtual environment](https://docs.python.org/3/library/venv.html) to manage Python packages for standalone applications like Deephaven. Install the wheel you downloaded, or use one of the alternatives below:

```bash
python3 -m venv deephaven-venv
source deephaven-venv/bin/activate

pip install "deephaven_core-${DH_VERSION}-py3-none-any.whl[autocomplete]"

# Or, with a link to wheels directory
# pip install --find-links path/to/wheels/ "deephaven-core[autocomplete]==${DH_VERSION}"

# Or, from PyPI
# pip install "deephaven-core[autocomplete]==${DH_VERSION}"
```

The `[autocomplete]` extra adds code completion in the IDE. See [Choose Python packages to install](../reference/cheat-sheets/choose-python-packages.md) for other extras.

Start the server from this activated shell. Activating the virtual environment sets the `VIRTUAL_ENV` environment variable, which Deephaven uses to find Python.

## Run the server

The server contains a `bin/` directory with a `start` script. To start the server, run the `start` script:

```bash
./server-jetty-${DH_VERSION}/bin/start
```

The `start` script passes the `START_OPTS` environment variable to the JVM as extra arguments.

The server prints its logs to the console, and the web UI is available at [http://localhost:10000](http://localhost:10000).

To stop the server, press Ctrl+C.

### Authentication

By default, Deephaven uses [pre-shared key authentication](../how-to-guides/authentication/auth-psk.md). If you don't set a key, the server generates a random key each time it starts and prints it to the console, like this:

![Logs display a randomly generated key](../assets/tutorials/default-psk.png)

Open the URL in the second log line, which includes the key, to sign in automatically.

To set your own pre-shared key, add `-Dauthentication.psk=YOUR_PASSWORD_HERE` to `START_OPTS`. The following command sets that key:

```bash
START_OPTS="-Dauthentication.psk=YOUR_PASSWORD_HERE" ./server-jetty-${DH_VERSION}/bin/start
```

![Logs display the user-defined key](../assets/how-to/custom-psk2.png)

> [!WARNING]
> [Anonymous authentication](../how-to-guides/authentication/auth-anon.md) is not secure. Use it only on a trusted network.

The following command uses anonymous authentication:

```bash
START_OPTS="-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler" ./server-jetty-${DH_VERSION}/bin/start
```

### Multiple servers

To run several servers on one machine with different Python requirements, set the virtual environment for each server explicitly:

- `VIRTUAL_ENV` selects the virtual environment.
- `DEEPHAVEN_APPLICATION` names the server and gives it its own configuration, cache, and data directories. See [Configure the production application](../how-to-guides/configuration/configure-production-application.md).
- `-Dhttp.port`, set in `START_OPTS`, gives each server its own port.

The following commands start two servers:

```bash
# Start "foo" with the virtual environment at /path/to/foo-venv
DEEPHAVEN_APPLICATION=foo VIRTUAL_ENV=/path/to/foo-venv START_OPTS="-Dhttp.port=10000" ./server-jetty-${DH_VERSION}/bin/start
```

```bash
# Start "bar" with the virtual environment at /path/to/bar-venv
DEEPHAVEN_APPLICATION=bar VIRTUAL_ENV=/path/to/bar-venv START_OPTS="-Dhttp.port=10001" ./server-jetty-${DH_VERSION}/bin/start
```

## Next steps

import { TutorialCTA } from '@theme/deephaven/CTA';

<div className="row">
<TutorialCTA to="/core/docs/getting-started/crash-course/overview" />
</div>

## Related documentation

- [Configure the production application](../how-to-guides/configuration/configure-production-application.md)
- [Pre-shared key authentication](../how-to-guides/authentication/auth-psk.md)
- [Choose Python packages to install](../reference/cheat-sheets/choose-python-packages.md)
- [Version matrix](../reference/version-matrix.md)
- [WSL 2 clock drift](../reference/community-questions/wsl2-clock-drift.md)
