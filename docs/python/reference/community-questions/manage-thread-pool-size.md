---
title: How can I control how many threads Deephaven uses for query processing?
---

Deephaven parallelizes query work across two thread pools, each sized by its own configuration property:

- `OperationInitializationThreadPool.threads` — threads used to initialize query operations in parallel
- `PeriodicUpdateGraph.updateThreads` — threads used to process table updates during each update graph cycle

Both default to `-1`, which tells Deephaven to use the number of processors available to the JVM (`Runtime.availableProcessors`). In a container, that number reflects the container's CPU limit. Any value less than or equal to zero has the same effect.

These properties control query-engine parallelism, not total JVM CPU use. Other threads (garbage collection, gRPC, etc.) are not governed by them.

Set them like any other configuration property, for example with `-DPeriodicUpdateGraph.updateThreads=4` in `START_OPTS`, or with `jvm_args=["-DPeriodicUpdateGraph.updateThreads=4"]` in the Python embedded server. See [How to set properties](../../how-to-guides/configuration/configuration-properties.md#how-to-set-properties).

For more on concurrency in Deephaven, see [Managing thread pool sizes](../../conceptual/query-engine/parallelization.md#managing-thread-pool-sizes).

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not answered in our documentation, [join our Community](/slack) and we'll be happy to help.
