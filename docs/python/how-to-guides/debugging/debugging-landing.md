---
id: debugging-landing
title: Debug Python code in Deephaven
---

import { CoreTutorialCard } from '@theme/deephaven/core-docs-components';

Deephaven supports debugging with several IDEs, whether you run it from Docker, from pip, or from source. This section shows how to debug Python code running in Deephaven from an external IDE, so you can step through code, inspect variables, and find problems.

## How debugging works with Deephaven

The Deephaven web IDE does not have a built-in debugger. To debug Python scripts and [user-defined functions](../python-functions.md), you need an external IDE.

Setting up a debugger depends on where the Deephaven engine runs. If it runs inside the Python process your IDE launches, the IDE's built-in debugger works locally. If it runs in a separate server process, you debug remotely. Where the engine runs depends on how you install Deephaven:

- **Docker**: The [Docker installation](../../getting-started/docker-install.md) runs the server in a container, separate from your IDE, so you debug remotely.
- **Pip installation**: How you start the server decides how you debug. A Python script that starts the server with the `deephaven-server` package from a [pip installation](../../getting-started/pip-install.md) runs the engine inside the Python process your IDE launches, so you debug locally. A server you start from a terminal with [`deephaven server`](../../getting-started/pip-install.md#from-the-command-line) and use from the web IDE runs outside your IDE, so you debug it remotely.
- **Server built from source**: A server [built from source](../../getting-started/launch-build.md) and started with `./gradlew server-jetty-app:run` runs the engine in its own process, separate from your IDE, so you debug remotely.
- **Embedded server built from source**: This is the same `deephaven-server` package that pip installs, built as a wheel from a local `deephaven-core` clone. A script that starts it runs the engine inside the Python process your IDE launches, so you debug locally.

For local debugging, the guides in this section use the built-in debugger in [PyCharm](https://www.jetbrains.com/pycharm/) (free or Pro) or [VS Code](https://code.visualstudio.com). For remote debugging, they use the remote debugging server in PyCharm Pro, the paid tier of PyCharm.

The PyCharm and VS Code debuggers in these guides step through Python code only. To step into Deephaven's Java engine, attach a JVM debugger such as [IntelliJ IDEA](https://www.jetbrains.com/idea/). The [source build guide](./source-setup.md#debug-deephavens-java-engine) and the [embedded server guide](./embedded-setup.md#debug-deephavens-java-engine) show how.

> [!NOTE]
> PyCharm 2025.1 and later ship as a single product, with free features and PyCharm Pro subscription features. Earlier releases shipped as two editions, PyCharm Community (free) and PyCharm Professional. If you use PyCharm Community 2025.2, the last standalone free release, follow the PyCharm (free) row of the table below.
>
> Use PyCharm 2024 or later. Debugging Deephaven with PyCharm 2023 has known problems. Use VS Code 1.94 or later.

## Debugging support by installation method

The following table shows debugging support for different Deephaven installation methods:

<table className="text--center">
  <thead>
    <tr>
      <th>Debugger</th>
      <th>Docker</th>
      <th>Pip installation</th>
      <th>Server built from source</th>
      <th>Embedded server built from source</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td scope="row">PyCharm (free)</td>
      <td>❌</td>
      <td>✅</td>
      <td>❌</td>
      <td>✅</td>
    </tr>
    <tr>
      <td scope="row">PyCharm Pro (paid)</td>
      <td>✅</td>
      <td>✅</td>
      <td>✅</td>
      <td>✅</td>
    </tr>
    <tr>
      <td scope="row">VS Code</td>
      <td>—</td>
      <td>✅</td>
      <td>—</td>
      <td>—</td>
    </tr>
  </tbody>
</table>

In the table:

- A check mark means that a guide in this section covers that IDE for that installation method.
- A cross means that the guide for that installation method uses PyCharm's remote debugging server, a PyCharm Pro feature, so the free tier can't follow it.
- A dash means that no guide in this section covers that IDE for that installation method.

On Windows, the pip and embedded server guides set up PyCharm with a [WSL](https://learn.microsoft.com/en-us/windows/wsl/install) interpreter, which requires PyCharm Pro. The PyCharm (free) check marks apply to macOS and Linux.

## Getting started

Choose the guide below that matches how you install and run Deephaven. Each guide shows you how to set up a debugger for the supported IDEs.

<div className="row">

<CoreTutorialCard to="/core/docs/how-to-guides/debugging/docker-setup/">

## Deephaven run from Docker

Use [PyCharm Pro](https://www.jetbrains.com/pycharm/)'s remote debugging server to debug Deephaven inside the container.

</CoreTutorialCard>

<CoreTutorialCard to="/core/docs/how-to-guides/debugging/pip-setup/">

## Deephaven installed with pip

Use [PyCharm](https://www.jetbrains.com/pycharm/) (free or Pro) or [VS Code](https://code.visualstudio.com) to debug scripts that run in pip-installed Deephaven. To debug a server started with [`deephaven server`](../../getting-started/pip-install.md#from-the-command-line), the guide uses PyCharm Pro.

</CoreTutorialCard>

<CoreTutorialCard to="/core/docs/how-to-guides/debugging/source-setup/">

## Deephaven server built from source

Use [PyCharm Pro](https://www.jetbrains.com/pycharm/)'s remote debugging server to debug a Deephaven server that you build from source and start with Gradle.

</CoreTutorialCard>

<CoreTutorialCard to="/core/docs/how-to-guides/debugging/embedded-setup/">

## Embedded server built from source

Build the `deephaven-server` wheel from a local `deephaven-core` clone and debug it locally with [PyCharm](https://www.jetbrains.com/pycharm/) (free or Pro).

</CoreTutorialCard>

</div>

After setup, see [Common problems](./common-problems.md) for Deephaven-specific debugging issues. For general debugger usage, refer to your IDE's debugging documentation.

## Related documentation

- [Debug Docker-run Deephaven with PyCharm](./docker-setup.md)
- [Debug pip-installed Deephaven](./pip-setup.md)
- [Debug built-from-source Deephaven with PyCharm](./source-setup.md)
- [Debug an embedded Deephaven server with PyCharm](./embedded-setup.md)
- [Common problems when debugging Deephaven](./common-problems.md)
- [Install Deephaven with Docker](../../getting-started/docker-install.md)
- [Install Deephaven with pip](../../getting-started/pip-install.md)
- [Build and run Deephaven from source](../../getting-started/launch-build.md)
- [Python functions in query strings](../python-functions.md)
