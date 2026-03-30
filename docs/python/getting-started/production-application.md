---
title: Install and run the Deephaven production application
sidebar_label: Production application
---

This guide shows you how to install and launch the Deephaven Community Core production application. The production application runs Deephaven from artifacts produced during each release cycle. It runs Deephaven without installing Docker or building from source code. It is the recommended way to run production applications that use Deephaven, hence its name. This guide only covers getting started with the production application. For more advanced configuration options, see [Configure the production application](../how-to-guides/configuration/configure-production-application.md).

## Supported operating systems

Deephaven is only supported on:

- Linux
- MacOS
- Windows 10 or 11 (requires [WSL 2 (Windows Subsystem for Linux v2)](https://learn.microsoft.com/en-us/windows/wsl/install))

## Prerequisites

[Java](https://www.java.com/en/) and [Python](https://www.python.org/) are the only required packages to run Deephaven's production application.

| Package | Recommended version | Required version |
| ------- | ------------------- | ---------------- |
| java    | latest LTS          | >= 17            |
| python  | latest LTS          | >= 3.9           |

## Get the artifacts

The Deephaven artifacts are attached to each [Deephaven release](https://github.com/deephaven/deephaven-core/releases). The latest release is available at [https://github.com/deephaven/deephaven-core/releases/latest](https://github.com/deephaven/deephaven-core/releases/latest). Only two of the artifacts are required to run Deephaven with Python:

- The server artifact in tar format.
- The Python artifact in wheel format.

It's recommended that you set your preferred version with an environment variable. The shell commands in this guide set and use the `DH_VERSION` environment variable:

```bash
export DH_VERSION=0.39.2
```

You can download release artifacts from your browser of choice or via the command line:

```bash
wget https://github.com/deephaven/deephaven-core/releases/download/v${DH_VERSION}/server-jetty-${DH_VERSION}.tar
wget https://github.com/deephaven/deephaven-core/releases/download/v${DH_VERSION}/deephaven_core-${DH_VERSION}-py3-none-any.whl
```

## Unpack the server

Once downloaded, the server artifact needs to be unpacked. You can do this from your file explorer or via the command line.

```bash
tar xvf server-jetty-${DH_VERSION}.tar
```

## Create the virtual environment

It is highly recommended to use a [virtual environment](https://docs.python.org/3/library/venv.html) to manage Python packages for standalone applications like Deephaven:

> [!NOTE]
> Activating a virtual environment sets an environment variable that tells Deephaven which venv to use.

```bash
python -m venv deephaven-venv
source deephaven-venv/bin/activate

pip install "deephaven_core-${DH_VERSION}-py3-none-any.whl[autocomplete]"

# Or, with link to wheels directory
# pip install --find-links path/to/wheels/ "deephaven-core[autocomplete]==${DH_VERSION}"

# Or, from PyPi
# pip install "deephaven-core[autocomplete]==${DH_VERSION}"
```

## Run the server

The server contains a `bin/` directory with a `start` script. To start the server, invoke the start script:

```bash
./server-jetty-${DH_VERSION}/bin/start
```

The server should now be up and running with stdout printed to the console and the web UI available at [http://localhost:10000](http://localhost:10000).

You can stop the server with `ctrl+C` or `cmd+C`.

### Authentication

Deephaven, by default, uses [pre-shared key authentication](../how-to-guides/authentication/auth-psk.md). If no key is set, a randomly generated key is used to log into the server each time it starts. The randomly generated key is printed to the Docker logs like this:

![Logs display a randomly generated key](../assets/tutorials/default-psk.png)

To set your own pre-shared key, add `-Dauthentication.psk=<YourPasswordHere>` to the `START_OPTS`. The following command uses `YOUR_PASSWORD_HERE` as the pre-shared key:

```bash
START_OPTS="-Dauthentication.psk=YOUR_PASSWORD_HERE" server-jetty-${DH_VERSION}/bin/start
```

![Logs display the user-defined key](../assets/how-to/custom-psk2.png)

The following command uses anonymous authentication:

```bash
# Careful, anonymous authentication is not secure.
START_OPTS="-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler" server-jetty-${DH_VERSION}/bin/start
```

### Multiple servers

Sourcing the Python virtual environment `activate` script has the side effect of setting the `VIRTUAL_ENV` environment variable, which Deephaven uses to select the Python virtual environment to use.

If you have multiple Deephaven servers on the same machine with different Python requirements, it may be best to be explicit about which Deephaven server uses which virtual environment:

```bash
# Start "foo" with the virtual environment at /path/to/foo-venv
DEEPHAVEN_APPLICATION=foo VIRTUAL_ENV=/path/to/foo-venv ./server-jetty-${DH_VERSION}/bin/start
```

```bash
# Start "bar" with the virtual environment at /path/to/bar-venv
DEEPHAVEN_APPLICATION=bar VIRTUAL_ENV=/path/to/bar-venv ./server-jetty-${DH_VERSION}/bin/start
```

## What to do next?

import { TutorialCTA } from '@theme/deephaven/CTA';

<div className="row">
<TutorialCTA to="/core/docs/getting-started/crash-course/overview" />
</div>

## Related documentation

- [Configure the production application](../how-to-guides/configuration/configure-production-application.md)
- [Choose Python packages to install](../reference/cheat-sheets/choose-python-packages.md)
