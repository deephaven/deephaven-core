---
id: debugging-landing
sidebar_label: Debugging Python code
title: Debug Python code in Deephaven
---

import { CoreTutorialCard } from '@theme/deephaven/core-docs-components';

Debugging is a critical part of software development. Deephaven supports debugging in many contexts with different IDEs, allowing you to step through code, analyze state, and find problems.

## How debugging works with Deephaven

Setting up a debugger in Deephaven depends on how Deephaven is running and which IDE you're using:

- **Pip installation**: Debug locally using standard IDE debuggers (PyCharm Community/Professional or VS Code). Your code runs in the same Python environment as your IDE.
- **Docker and source installations**: Debug remotely using PyCharm Professional's remote debugging server. Code runs in a container or separate process, requiring a remote connection.

The Deephaven web IDE does not have a built-in debugger. You must use an external IDE to debug Python scripts and user-defined functions.

## Debugging support by installation method

The following table shows current debugging support for different Deephaven installation methods:

<table className="text--center">
  <thead>
    <tr>
      <th> Debugger IDE</th>
      <th>Docker</th>
      <th>Pip</th>
      <th>Source</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td scope="row">PyCharm Community</td>
      <td>❌</td>
      <td>✅</td>
      <td>❌</td>
    </tr>
    <tr>
      <td scope="row">PyCharm Professional</td>
      <td>✅</td>
      <td>✅</td>
      <td>✅</td>
    </tr>
    <tr>
      <td scope="row">VS Code</td>
      <td>❌</td>
      <td>✅</td>
      <td>❌</td>
    </tr>
  </tbody>
</table>

> [!NOTE]
> **PyCharm Professional** is a paid product with remote debugging capabilities. **PyCharm Community** is free but only supports local debugging (pip installation only).

> [!WARNING]
> PyCharm debugging has only been verified to work with **PyCharm 2024 or higher**. Earlier versions may have compatibility issues.

## Getting started

The guides in this section are organized by installation method. Choose the guide that matches how you installed Deephaven: [Docker](./docker-setup.md), [pip](./pip-setup.md), or [source](./source-setup.md). Each guide shows you how to set up a debugger for the supported IDEs.

After setup, see [Common problems](./common-problems.md) for Deephaven-specific debugging issues you may encounter. For general debugger usage, refer to your IDE's debugging documentation.

<div className="row">

<CoreTutorialCard to="/core/docs/how-to-guides/debugging/docker-setup/">

## Deephaven run from Docker

Use PyCharm Professional's remote debugging server to debug Deephaven inside the container.

</CoreTutorialCard>

<CoreTutorialCard to="/core/docs/how-to-guides/debugging/pip-setup/">

## Deephaven installed with pip

Use PyCharm Community, PyCharm Professional, or VS Code to debug scripts using pip-installed Deephaven.

</CoreTutorialCard>

<CoreTutorialCard to="/core/docs/how-to-guides/debugging/source-setup/">

## Deephaven built from source

Use PyCharm Professional's remote debugging server to debug versions of Deephaven that are built from source code.

</CoreTutorialCard>

</div>
