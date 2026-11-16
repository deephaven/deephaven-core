---
id: debugging-landing
title: Debug Groovy code in Deephaven
---

import { CoreTutorialCard } from '@theme/deephaven/core-docs-components';

This section shows how to attach an external IDE's debugger to the JVM that runs Deephaven, so you can set breakpoints in Deephaven's Java code, step through the Java methods your Groovy scripts call, and inspect variables.

## How debugging works with Deephaven

The Deephaven web IDE does not have a built-in debugger. To use a debugger, you need an external IDE or another tool that can attach a debugger to a JVM.

Groovy code in Deephaven runs on the JVM, and you attach a debugger to that JVM over the [Java Debug Wire Protocol](https://docs.oracle.com/en/java/javase/21/docs/specs/jdwp/jdwp-spec.html), or JDWP. JDWP is part of the JVM, so you don't need to install any extra packages. To debug:

1. Start the Deephaven server with JDWP enabled.
2. Connect a compatible IDE debugger to the debug port that the server exposes.

Deephaven compiles each Groovy script at runtime into a class with a generated name, and adds a line at the top. The debugger can't match that class to your `.groovy` file, so it doesn't stop at breakpoints in your scripts, whether you type them into the console or load them with [Application Mode](../application-mode.md). Set breakpoints in the Java code your script calls instead, or trace script logic with `println`. See [Breakpoints in Groovy scripts are not hit](./common-problems.md#breakpoints-in-groovy-scripts-are-not-hit).

The guides in this section use [IntelliJ IDEA](https://www.jetbrains.com/idea/), which has built-in Groovy support. Any other IDE or tool that supports JDWP can also connect to the debug port.

> [!NOTE]
> IntelliJ IDEA 2025.3 and later ship as a single product, with free and paid features. Earlier releases shipped as Community (free) and Ultimate (paid). Remote JVM Debug is available in every edition of IntelliJ IDEA, including the free one.

## Getting started

The guides cover two ways to run Deephaven: [Docker](../../getting-started/docker-install.md) and a [server built from source](../../getting-started/launch-build.md). Choose the guide below that matches how you run Deephaven. Each shows how to start the server with JDWP enabled and connect [IntelliJ IDEA](https://www.jetbrains.com/idea/).

<div className="row">

<CoreTutorialCard to="/core/groovy/docs/how-to-guides/debugging/docker-setup/">

## Deephaven run from Docker

Enable JDWP in your Docker configuration and connect IntelliJ IDEA's remote debugger to the container.

</CoreTutorialCard>

<CoreTutorialCard to="/core/groovy/docs/how-to-guides/debugging/source-setup/">

## Deephaven built from source

Start the server you built from source with the `-Pgroovy` and `-Pdebug` Gradle flags and attach IntelliJ IDEA's remote debugger.

</CoreTutorialCard>

</div>

After setup, see [Common problems](./common-problems.md) for Deephaven-specific debugging issues. For general debugger usage, refer to your IDE's debugging documentation.

## Related documentation

- [Debug Docker-run Deephaven with IntelliJ IDEA](./docker-setup.md)
- [Debug built-from-source Deephaven with IntelliJ IDEA](./source-setup.md)
- [Common problems when debugging Deephaven](./common-problems.md)
- [Install Deephaven with Docker](../../getting-started/docker-install.md)
- [Build and run Deephaven from source](../../getting-started/launch-build.md)
- [Application Mode](../application-mode.md)
