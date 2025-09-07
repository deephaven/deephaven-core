---
title: Periodic Update Graph
sidebar_label: Periodic Update Graph configuration
---

This guide discusses how to control the update frequency of your Deephaven tables, which allows you to fine-tune your application's latency.

There are two underlying processes when updating streaming tables in Deephaven:

1. When new data is generated or acquired, the Periodic Update Graph (PUG) first manages the process of updating the source tables and the derived tables with the new data. At this point, data updates are available within the server process.
2. Once a table has been updated during the PUG refresh cycle, the Viewport Update process sends the relevant data to the client, which then displays it in the user interface.

![img](../assets/conceptual/ugp-cycle.png)

## Periodic Update Graph

The Periodic Update Graph is the update graph that ultimately controls how often data is refreshed on the server. At the beginning of a cycle, the PUG thread refreshes each source table. When a source table has new data, it enqueues a notification of the changes for all registered listeners. Most listeners in the Deephaven system generate derived tables and send further notifications to their children. Listeners also send table updates across the wire using Barrage, perform custom user logic, and perform other incremental processing tasks. Independent notifications are executed in parallel, beginning with the source tables. This allows Deephaven operations to be optimized for high-single-threaded performance but achieve concurrency by allowing multiple threads to execute distinct operations at once. By default, the property `PeriodicUpdateGraph.updateThreads` is set to `-1`, which uses all available processors for concurrent work. Users may set this property to cap the number of threads used for derived table updates. The refresh cycle is completed after all notifications are processed.

An illustration of the process is below:

![img](../assets/conceptual/ugp-refresh-cycle.png)

The Periodic Update Graph has five user-configurable properties:

- `PeriodicUpdateGraph.updateThreads`
- `PeriodicUpdateGraph.targetCycleDurationMillis`
- `PeriodicUpdateGraph.minimumInterCycleSleep`
- `PeriodicUpdateGraph.interCycleYield`
- `PeriodicUpdateGraph.allowUnitTestMode`

Each property is described below.

### `updateThreads`

The `PeriodicUpdateGraph.updateThreads` property allows multithreading within the PUG refresh cycle. By default, this property is set to `-1`, which uses all available processors for concurrent work.

For example, consider the workflow given in the [image above](#periodic-update-graph). In this example, data flows from the live source tables into derived tables -- tables `A`, `B`, and `C` first. Once all three tables have been updated, all derived tables are updated. This flow minimizes latency.

To take advantage of multithreading, queries can be structured to use [`partition_by`](../how-to-guides/partitioned-tables.md), which partitions a table into constituent tables defined by key-value pairs similar to a map. This enables parallelism in the PUG cycle by allowing the map to be operated on as if it were a single table. Then, any table operation will be applied to each constituent across multiple threads. At the end, [`merge`](../reference/table-operations/partitioned-tables/merge.md) can combine the constituent tables back into a single table.

Care should be taken to ensure that parallelism is performed along natural boundaries in data and that data is structured to allow for efficient parallel processing. For a deeper dive into parallelism in Deephaven with partitioned tables, see [the how-to guide](../how-to-guides/partitioned-tables.md#parallelization).

### `targetCycleDurationMillis`

The `PeriodicUpdateGraph.targetCycleDurationMillis` sets the target time in milliseconds for one PUG refresh cycle to complete. The default value is 1000 milliseconds (one second), which aligns with the Deephaven UI's default refresh period.

Changing this value changes the PUG's refresh cycle frequency. Setting this to 500 milliseconds means the PUG refresh cycle will run twice as often as the default.

The value set for this property does not guarantee the actual time for the cycle to complete. If the actual refresh cycle completes faster than the value set, the next refresh cycle does _not_ start until the full cycle duration has elapsed. If the actual refresh cycle takes longer than the value set, the refresh cycle starts again immediately after the previous cycle completes, falling behind the target cycle duration.

A variety of conditions can cause the actual refresh cycle to take longer than the target cycle duration, including:

- The actual time needed for incremental update processing from refreshed tables may be longer than anticipated.
- The PUG refresh thread may contend with the PUG lock, which prevents data refresh during the middle of an operation. Consider two threads changing the same data at the same time. Placing a lock on the data prevents another thread from touching that data until the lock is released.
- If the server is busy, the refresh thread may not be scheduled.
- Configuration settings such as [`minimumInterCycleSleep`](#minimumintercyclesleep) may cause the cycle to be longer than the target duration.

### `minimumInterCycleSleep`

The `PeriodicUpdateGraph.minimumInterCycleSleep` property sets the minimum amount of time in milliseconds to wait before starting another refresh cycle after the current cycle has completed. The default value is 0.

If the actual cycle time plus the `minimumInterCycleSleep` time value is greater than the `targetCycleDurationMillis`, the next cycle will start as soon as the `minimumInterCycleSleep` has elapsed.

If the actual cycle time plus the `minimumInterCycleSleep` time value is less than the `targetCycleDurationMillis`, the next cycle will begin after `targetCycleDurationMillis` has elapsed.

Consider a situation where `targetCycleDurationMillis` is set to its default value of 1000 ms. The actual time to complete a particular refresh cycle is 900 ms. If `minimumInterCycleSleep` is set to 0 (the default), the next refresh cycle will start 100 ms later. If `minimumInterCycleSleep` is set to 200 ms, the next cycle will start 1100 ms (900 ms for the cycle time + 200 ms sleep time) after the current cycle starts.

### `interCycleYield`

The `PeriodicUpdateGraph.interCycleYield` property sets whether the thread should be allowed to yield the CPU in between refresh cycles. By default, it is `false`, which causes the PUG to immediately begin the next PUG cycle once the current cycle ends. If set to `true`, the PUG process yields to the CPU between cycles.

Use of this property is designed to reduce latency while preventing the PUG from starving other threads.

### `allowUnitTestMode`

The `PeriodicUpdateGraph` supports executing within context of a unit test. When this property is set to `true`, added sources are ignored and refresh actions are manually taken by the test harness. The PUG may not be started. This should never be used outside the context of a unit test. As such, the default value is `false`.

## Operation Initializer Thread Pool

The Operation Initializer Thread Pool has one user-configurable property:

- `OperationInitializerThreadPool.threads`

### `threads`

The `OperationInitializerThreadPool.threads` property allows multithreading during new operation initiation. By default, this property is set to `-1`, which uses all available processors for concurrent initiation work.

## Related documentation

- [Partitioned tables](../how-to-guides/partitioned-tables.md)
- [Deephaven's design](./deephaven-design.md)
- [Incremental update model](./table-update-model.md)
- [The Deephaven DAG](./dag.md)
- [Core API design](./deephaven-core-api.md)
- [Multithreading in Deephaven](./query-engine/engine-locking.md)
- [Parallelizing queries](./query-engine/parallelization.md)
