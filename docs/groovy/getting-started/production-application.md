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

[Java](https://www.java.com/en/) is the only required packages to run Deephaven's production application. If you wish to run a Deephaven Python server, you will also need [Python](https://www.python.org/) 3.9 or later.

| Package | Recommended version | Required version |
| ------- | ------------------- | ---------------- |
| java    | latest LTS          | >= 17            |

## Get the artifacts

The Deephaven artifacts are attached to each [Deephaven release](https://github.com/deephaven/deephaven-core/releases). The latest release is available at [https://github.com/deephaven/deephaven-core/releases/latest](https://github.com/deephaven/deephaven-core/releases/latest). Only one of the artifacts is required to run the production application:

- The server artifact in tar format.

It's recommended that you set your preferred version with an environment variable. The shell commands in this guide set and use the `DH_VERSION` environment variable:

```bash
export DH_VERSION=0.39.2
```

You can download release artifacts from your browser of choice or via the command line:

```bash
wget https://github.com/deephaven/deephaven-core/releases/download/v${DH_VERSION}/server-jetty-${DH_VERSION}.tar
```

## Unpack the server

Once downloaded, the server artifact needs to be unpacked. You can do this from your file explorer or from the command line.

```bash
tar xvf server-jetty-${DH_VERSION}.tar
```

## Run the server

The server contains a `bin/` directory with a `start` script. To start the server, invoke the start script:

```bash
START_OPTS="-Ddeephaven.console.type=groovy" ./server-jetty-${DH_VERSION}/bin/start
```

The server should now be up and running with stdout printed to the console and the web UI available at [http://localhost:10000](http://localhost:10000).

You can stop the server with `ctrl+C` or `cmd+C`.

### Authentication

Deephaven, by default, uses [pre-shared key authentication](../how-to-guides/authentication/auth-psk.md). If no key is set, a randomly generated key is used to log into the server each time it starts. The randomly generated key is printed to the Docker logs like this:

![Logs display a randomly generated key](../assets/tutorials/default-psk.png)

To set your own pre-shared key, add `-Dauthentication.psk=<YourPasswordHere>` to the `START_OPTS`. The following command uses `YOUR_PASSWORD_HERE` as the pre-shared key:

```bash
START_OPTS="-Dauthentication.psk=YOUR_PASSWORD_HERE -Ddeephaven.console.type=groovy" server-jetty-${DH_VERSION}/bin/start
```

![Logs display the user-defined key](../assets/how-to/custom-psk2.png)

The following command uses anonymous authentication:

```bash
# Careful, anonymous authentication is not secure.
START_OPTS="-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler -Ddeephaven.console.type=groovy" server-jetty-${DH_VERSION}/bin/start
```

## What to do next?

import { TutorialCTA } from '@theme/deephaven/CTA';

<div className="row">
<TutorialCTA to="/core/groovy/docs/getting-started/crash-course/overview" />
</div>

## Related documentation

- [Configure the production application](../how-to-guides/configuration/configure-production-application.md)
