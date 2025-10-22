---
id: debugging-landing
title: Debug Python code in Deephaven
---

import { CoreTutorialCard } from '@theme/deephaven/core-docs-components';

Debugging is a critical part of software development. Deephaven supports debugging in many contexts with different IDEs, allowing you to step through code, analyze state, and find problems.

Setting up a debugger in Deephaven depends on how Deephaven is running and which IDE you're using. The Deephaven IDE itself does not have a debugger, so you must use remote debuggers from other IDEs to debug scripts hosted in the Deephaven IDE. The following table shows current debugging support for different Deephaven installation methods:

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
      <td scope="row" ><a>PyCharm Community</a></td>
      <td>❌</td>
      <td>✅</td>
      <td>❌</td>
    </tr>
    <tr>
      <td scope="row" ><a>PyCharm Professional</a></td>
      <td>✅</td>
      <td>✅</td>
      <td>✅</td>
    </tr>
    <tr>
      <td scope="row" ><a>VS Code</a></td>
      <td>❌</td>
      <td>✅</td>
      <td>❌</td>
    </tr>
  </tbody>
</table>

The guides in this section are organized by installation method. Choose the guide that matches how you installed Deephaven: [Docker](./docker-setup.md), [pip](./pip-setup.md), or [source](./source-setup.md). Each guide shows you how to set up a debugger for the supported IDEs. For information on using the debugger after setup, refer to your IDE's debugging documentation.

<CoreTutorialCard to="/core/docs/how-to-guides/debugging/docker-setup/">

## Deephaven run from Docker

Use PyCharm Professional's remote debugging server to debug Deephaven inside the container.

</CoreTutorialCard>

<CoreTutorialCard to="/core/docs/how-to-guides/debugging/pip-setup/">

## Deephaven installed with pip

Use PyCharm Community, PyCharm Professional, or VS Code to debug scripts using pip-installed Deephaven.

</CoreTutorialCard>

<CoreTutorialCard to="core/docs/how-to-guides/debugging/source-setup/">

## Deephaven built from source

Use PyCharm Professional's remote debugging server to debug versions of Deephaven that are built from source code.

</CoreTutorialCard>
