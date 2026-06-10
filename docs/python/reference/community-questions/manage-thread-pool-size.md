---
title: How can I manage the number of cores available to the thread pool?
sidebar_label: How can I manage the number of cores Deephaven can use?
---

Deephaven offers two properties to manage the number of cores available to the thread pool:

- `OperationInitializationThreadPool.threads`
- `PeriodicUpdateGraph.updateThreads`

By default, both of these are set to `-1`, which means that Deephaven will use the number of available processors on your machine. For more on concurrency in Deephaven, see [Parallelizing queries](../../conceptual/query-engine/parallelization.md).
