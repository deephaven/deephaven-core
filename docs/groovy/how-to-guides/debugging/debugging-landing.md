---
id: debugging-landing
sidebar_label: Debugging Groovy code
title: Debug Groovy code in Deephaven
---

import { CoreTutorialCard } from '@theme/deephaven/core-docs-components';

Debugging is a critical part of software development. Deephaven supports debugging Groovy code through the JVM's built-in remote debugging protocol, allowing you to step through code, inspect variables, and find problems from your IDE.

## How debugging works with Deephaven

Groovy code in Deephaven runs on the JVM. Debugging uses [JDWP](https://docs.oracle.com/en/java/javase/17/docs/specs/jdwp/jdwp-spec.html) (Java Debug Wire Protocol), which is built into the JVM — no additional packages are needed. You start the Deephaven server with JDWP enabled, then connect a compatible IDE debugger to the exposed debug port.

This guide uses [IntelliJ IDEA](https://www.jetbrains.com/idea/), which has first-class Groovy support. Both Community (free) and Ultimate editions work.

The Deephaven web IDE does not have a built-in debugger. You must use an external IDE to debug Groovy scripts.

## Debugging support by installation method

| Debugger                | Docker | Source |
| ----------------------- | ------ | ------ |
| IntelliJ IDEA Community | ✅     | ✅     |
| IntelliJ IDEA Ultimate  | ✅     | ✅     |

> [!NOTE]
> Any IDE or tool that supports JDWP can connect to the debug port. This guide focuses on IntelliJ IDEA because of its excellent Groovy tooling.

## Getting started

Choose the guide that matches how you installed Deephaven: [Docker](./docker-setup.md) or [source](./source-setup.md). Each guide shows how to start the server with JDWP enabled and connect IntelliJ IDEA.

After setup, see [Common problems](./common-problems.md) for Deephaven-specific debugging issues.

<div className="row">

<CoreTutorialCard to="/core/docs/how-to-guides/debugging/docker-setup/">

## Deephaven run from Docker

Enable JDWP in your Docker configuration and connect IntelliJ IDEA's remote debugger to the container.

</CoreTutorialCard>

<CoreTutorialCard to="/core/docs/how-to-guides/debugging/source-setup/">

## Deephaven built from source

Start the source build with the `-Pdebug` Gradle flag and attach IntelliJ IDEA's remote debugger.

</CoreTutorialCard>

</div>

## Related documentation

- [Docker debugging setup](./docker-setup.md)
- [Source debugging setup](./source-setup.md)
- [Common debugging problems](./common-problems.md)
