---
title: "Multithreading: Synchronization, locks, and snapshots"
---

Deephaven often runs on multiprocessor systems, where multiple threads read and change the same data. The
[UpdateGraph](../table-update-model.md) is the component that propagates table updates. Ticking tables change each time
it runs an update cycle, and other threads may be reading those tables at the same time. Deephaven synchronizes that
access with three mechanisms:

- A **shared lock** lets threads run table operations and read data while no update cycle is running.
- An **exclusive lock** gives one thread sole access. The UpdateGraph holds it while it propagates changes.
- An [optimistic](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) **snapshotting** mechanism retrieves data and runs table operations without holding a lock. It falls back to the shared lock only when it can't get a consistent result.

## Choosing between snapshots and locks

The right choice depends on the operation and on the thread that runs it. Use these guidelines:

- Skip these mechanisms for most code you run in the Deephaven console, because Deephaven holds the exclusive lock while it runs console commands.
- Use **[snapshots](#snapshotting)** to read data from ticking tables, especially for a user interface. The Deephaven web IDE uses snapshots for this reason. A snapshot can run while the [UpdateGraph](../table-update-model.md) is updating, and it often returns sooner than a thread could acquire a lock. The [`snapshot`](../../reference/table-operations/snapshot/snapshot.md) table operation uses this mechanism, so you can call it from your own thread to get a static copy of a ticking table without acquiring a lock first.
- Use the **[shared lock](#shared-lock)** to run table operations from a thread that does not already hold a lock. The shared lock lets multiple threads run at once, so prefer it to the exclusive lock.
  - Some operations, such as [`where`](../../reference/table-operations/filter/where.md) and [`sort`](../../reference/table-operations/sort/sort.md), initialize from a snapshot and don't need a lock.
  - Others, such as [`update`](../../reference/table-operations/select/update.md) and [`naturalJoin`](../../reference/table-operations/join/natural-join.md), must run under a lock when any input table is refreshing.
- Use the **[exclusive lock](#exclusive-lock)** in advanced cases that must [wait for the UpdateGraph to process changes](#waiting-for-updategraph-changes-with-the-exclusive-lock) and that [`awaitUpdate`](../../reference/table-operations/table-listeners/await-update.md) can't handle.

## Query engine locks

The [UpdateGraph's](../table-update-model.md) [shared lock](https://deephaven.io/core/javadoc/io/deephaven/engine/updategraph/UpdateGraph.html#sharedLock())
and [exclusive lock](https://deephaven.io/core/javadoc/io/deephaven/engine/updategraph/UpdateGraph.html#exclusiveLock())
synchronize access to Deephaven data. By default, the UpdateGraph is the
[Periodic Update Graph](../periodic-update-graph-configuration.md), which runs an update cycle at a regular interval.
The UpdateGraph holds the exclusive lock for the whole update cycle, so no other thread can hold either lock while a
cycle runs.

Acquire locks only when necessary and hold them for the shortest duration possible. The UpdateGraph cannot start an
update cycle until every other thread has released them.

### Shared lock

Hold the **shared lock** to run table operations, such as [`update`](../../reference/table-operations/select/update.md) and [`naturalJoin`](../../reference/table-operations/join/natural-join.md), from your own threads.

You can also hold the shared lock to read large amounts of data from tables. [Snapshotting](#snapshotting) is the better choice when the read finishes before the next update cycle starts, because it doesn't block the UpdateGraph. A read that spans the start or end of an update cycle is inconsistent. After the attempts set in [Configuration](#configuration), the snapshotting mechanism falls back to the shared lock.

How long a thread waits for the shared lock depends on what the UpdateGraph and other threads are doing:

- During an update cycle, the thread waits until the cycle completes and the UpdateGraph releases the exclusive lock.
- Between cycles, if no other thread holds or is waiting for the exclusive lock, the thread acquires the shared lock without waiting.
- If another thread holds or is waiting for the exclusive lock, the thread waits. That other thread can be the
  UpdateGraph waiting to start its next cycle, or a running console command.

A thread waits behind a queued exclusive-lock request because a fair
[`ReentrantReadWriteLock`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/locks/ReentrantReadWriteLock.html)
backs both locks, and a fair lock grants waiting threads the lock in roughly arrival order. The backing lock
is an implementation detail and may change.

### Exclusive lock

Only one thread at a time can hold the **exclusive lock**, and no other thread can hold the shared lock while the
exclusive lock is held. Deephaven also holds the exclusive lock automatically while it runs commands in the Deephaven
console or an [Application Mode](../../reference/app-mode/application-mode-config.md) startup script.

You rarely hold the exclusive lock yourself. Its main use is to
[wait for changes from the UpdateGraph](#waiting-for-updategraph-changes-with-the-exclusive-lock), which this page covers after it shows how to
acquire a lock.

### Using locks

The preferred way to use the shared lock or the exclusive lock is as a
[functional lock](https://deephaven.io/core/javadoc/io/deephaven/util/locks/FunctionalLock.html). You pass the code to run under a
functional lock as an argument, typically a lambda expression, and the functional lock acquires and releases the lock
around it. This simplifies the standard `try`/`finally` locking pattern shown in
[Explicit locking and unlocking](#explicit-locking-and-unlocking).

The [`doLocked`](https://deephaven.io/core/javadoc/io/deephaven/util/locks/FunctionalLock.html) method takes a
[`ThrowingRunnable`](https://deephaven.io/core/javadoc/io/deephaven/util/function/ThrowingRunnable.html) and runs it while the thread holds the lock:

```groovy ticking-table order=null
import io.deephaven.engine.table.vectors.ColumnVectors;

Table myTable = timeTable("PT1s").update("MyCol = `A` + ii");
myTable.getUpdateGraph().sharedLock().doLocked( () -> {
  // Retrieve the data from "MyCol" as an array and print it:
  String[] myColAsArray = ColumnVectors.ofObject(myTable, "MyCol", String.class).toArray();
  System.out.println(Arrays.toString(myColAsArray));
});
```

Run as one console command, this example prints `[]`. The time table adds its first row on the next update cycle, which can't start until the command finishes.

To compute a value while holding the lock and return it to the caller, use
[`computeLocked`](https://deephaven.io/core/javadoc/io/deephaven/util/locks/FunctionalLock.html), which takes a
[`ThrowingSupplier`](https://deephaven.io/core/javadoc/io/deephaven/util/function/ThrowingSupplier.html):

```groovy ticking-table order=null
import io.deephaven.engine.table.vectors.ColumnVectors;

Table myTable = timeTable("PT1s").update("MyCol = `A` + ii");
String[] myColAsArray = myTable.getUpdateGraph().sharedLock().computeLocked( () -> {
  // Retrieve the data from "MyCol" as an array and return it:
  return ColumnVectors.ofObject(myTable, "MyCol", String.class).toArray();
});
```

Both [`doLocked`](https://deephaven.io/core/javadoc/io/deephaven/util/locks/FunctionalLock.html) and [`computeLocked`](https://deephaven.io/core/javadoc/io/deephaven/util/locks/FunctionalLock.html) let the passed function throw [checked exceptions](https://docs.oracle.com/javase/specs/jls/se11/html/jls-11.html#jls-11.1.1), such as `IOException` and `TimeoutException`. A checked exception is any exception that is not a subclass of `RuntimeException`. Both methods rethrow it to the caller, which must catch or declare it in Java. Groovy doesn't require this, but catching it keeps the error handling explicit:

```groovy ticking-table should-fail
Table myTable = timeTable("PT1s").update("MyCol = `A` + ii");
try {
  myTable.getUpdateGraph().sharedLock().computeLocked(() -> { throw new IOException(); });
} catch (IOException e) {
  throw new UncheckedIOException("IOException occurred during computeLocked()", e);
}
```

The `catch` block rethrows the `IOException` inside an `UncheckedIOException`, so this example ends with that error.

#### Explicit locking and unlocking

You can also acquire and release a lock explicitly with the
[`lock`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/locks/Lock.html#lock()) and
[`unlock`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/locks/Lock.html#unlock())
methods, but functional locks are safer. If you lock explicitly, call [`unlock`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/locks/Lock.html#unlock()) in a `finally` block so the lock is
released even when an exception or error occurs. The
[`doLocked`](https://deephaven.io/core/javadoc/io/deephaven/util/locks/FunctionalLock.html) and
[`computeLocked`](https://deephaven.io/core/javadoc/io/deephaven/util/locks/FunctionalLock.html) methods do this for you.

The following example shows the correct way to use the shared lock with explicit locking:

```groovy ticking-table order=null
Table myTable = timeTable("PT1s").update("MyCol = `A` + ii");
// Acquire the shared lock
myTable.getUpdateGraph().sharedLock().lock();
try {
  // code to run under the lock
} finally {
  myTable.getUpdateGraph().sharedLock().unlock();
}
```

### Waiting for UpdateGraph changes with the exclusive lock

You can use the exclusive lock to wait for changes from the UpdateGraph.

An operation needs to wait when it depends on an UpdateGraph cycle that happens partway through it. For example,
suppose your query flushes data to a
[`DynamicTableWriter`](../../reference/table-operations/create/DynamicTableWriter.md) during initialization. The
output table does not show that data until the UpdateGraph runs an update cycle.

The most common way to wait is [`awaitUpdate`](../../reference/table-operations/table-listeners/await-update.md). Calling it on the output table
acquires the exclusive lock and waits until that cycle has run, after which the flushed rows are visible. While it
waits, it releases the lock, so the UpdateGraph keeps running cycles.

> [!NOTE]
> Deephaven doesn't support upgrading from the shared lock to the exclusive lock. Attempting it throws an exception.
> Accordingly, you can't call [`awaitUpdate`](../../reference/table-operations/table-listeners/await-update.md) while your thread holds the shared lock.

When [`awaitUpdate`](../../reference/table-operations/table-listeners/await-update.md) isn't enough, an independent thread can hold the exclusive lock and wait on a
[condition](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/locks/Condition.html)
that the UpdateGraph signals. Use this pattern when a thread that the UpdateGraph doesn't run must wait for a change or
notification that the UpdateGraph propagates. Pair it with a custom
[listener](../../how-to-guides/table-listeners-groovy.md). The pattern has three steps:

1. Obtain a condition with `updateGraph.exclusiveLock().newCondition()`, where `updateGraph` is the UpdateGraph you capture from the table or [execution context](../execution-context.md) before you start the independent thread.
2. In the independent thread, acquire the exclusive lock and [`await`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/locks/Condition.html#await()) the condition. [`await`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/locks/Condition.html#await()) releases the lock while the thread waits and reacquires it before returning.
3. From code that the UpdateGraph runs, such as a [listener's](../../how-to-guides/table-listeners-groovy.md) [`onUpdate`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/TableUpdateListener.html) method, call
   [`requestSignal`](https://deephaven.io/core/javadoc/io/deephaven/engine/updategraph/UpdateGraph.html#requestSignal(java.util.concurrent.locks.Condition)).
   The UpdateGraph signals the condition while it holds the exclusive lock. A listener must not acquire either lock
   during an update cycle, and attempting it throws an exception. That's why the listener asks the UpdateGraph to
   signal the condition instead of signaling it directly.

The following Groovy example shows this pattern:

<!-- skip-test: the background thread and listener keep running after the snippet ends. -->

```groovy skip-test
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.updategraph.UpdateGraph;

import java.util.concurrent.locks.Condition;

// Capture the update graph on the script thread; a new thread has no execution context
final UpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph();
final Condition myCondition = updateGraph.exclusiveLock().newCondition();

Thread independentThread = new Thread(() -> {
  System.out.println(Thread.currentThread().getName() + " - " + now() + " - Acquiring update graph exclusive lock...");
  updateGraph.exclusiveLock().doLocked(() -> {
    System.out.println(Thread.currentThread().getName() + " - " + now() + " - Waiting for condition...");
    myCondition.await();
    System.out.println(Thread.currentThread().getName() + " - " + now() + " - Condition was signaled!");
  });
  System.out.println(Thread.currentThread().getName() + " - " + now() + " - update graph exclusive lock released.");

}, "myIndependentThread");
independentThread.start();

// Create a time table that ticks every 5 seconds
myTable = timeTable("PT5s");

myListener = new InstrumentedTableUpdateListenerAdapter(myTable, false) {
  @Override
  public void onUpdate(TableUpdate upstream) {
    // Request that the UpdateGraph signal the condition, so that anything
    // await()ing the condition is notified.
    System.out.println(Thread.currentThread().getName() + " - " + now() + " - Signaling condition...");
    updateGraph.requestSignal(myCondition);
    System.out.println(Thread.currentThread().getName() + " - " + now() + " - Condition signaled.");
  }
};

myTable.addUpdateListener(myListener);
```

With the default number of [update threads](../periodic-update-graph-configuration.md#updatethreads) on a multicore
machine, the UpdateGraph runs the listener on one of its update threads, so the
`Signaling condition...` and `Condition signaled.` lines show a thread name such as
`PeriodicUpdateGraph-updateExecutor-1`. The following output comes from a server configured with a single
update thread, so those lines show
`PeriodicUpdateGraph.DEFAULT.refreshThread`:

```
myIndependentThread - 2022-10-12T16:36:06.890861Z - Acquiring update graph exclusive lock...
myIndependentThread - 2022-10-12T16:36:06.998700Z - Waiting for condition...
PeriodicUpdateGraph.DEFAULT.refreshThread - 2022-10-12T16:36:07.142583Z - Signaling condition...
PeriodicUpdateGraph.DEFAULT.refreshThread - 2022-10-12T16:36:07.150394Z - Condition signaled.
myIndependentThread - 2022-10-12T16:36:07.153525Z - Condition was signaled!
myIndependentThread - 2022-10-12T16:36:07.154153Z - update graph exclusive lock released.
PeriodicUpdateGraph.DEFAULT.refreshThread - 2022-10-12T16:36:10.117031Z - Signaling condition...
PeriodicUpdateGraph.DEFAULT.refreshThread - 2022-10-12T16:36:10.117912Z - Condition signaled.
PeriodicUpdateGraph.DEFAULT.refreshThread - 2022-10-12T16:36:15.115583Z - Signaling condition...
PeriodicUpdateGraph.DEFAULT.refreshThread - 2022-10-12T16:36:15.116385Z - Condition signaled.
```

The output shows the progress of the two threads:

1. `myIndependentThread` acquires the exclusive lock and waits for the condition. Waiting releases the lock until the condition is signaled.
2. The UpdateGraph processes a [`timeTable`](../../reference/table-operations/create/timeTable.md) tick, which triggers the listener's [`onUpdate`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/TableUpdateListener.html) method.
3. [`onUpdate`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/TableUpdateListener.html) requests a signal for the condition.
4. After the UpdateGraph finishes its cycle, `myIndependentThread` returns from [`await`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/locks/Condition.html#await()) and resumes.

The [time table](../../how-to-guides/time-table.md) adds its first row on the first update cycle after it is created, and later rows on five-second
boundaries. The listener keeps running until you
[remove it](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#removeUpdateListener(io.deephaven.engine.table.TableUpdateListener))
from the table or it is garbage collected.

## Snapshotting

A **snapshot** is a guaranteed-consistent result derived from data in Deephaven without necessarily acquiring a lock.
Deephaven's snapshotting mechanism is a form of
[optimistic concurrency](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) that helps to retrieve data
safely, quickly, and without blocking the [UpdateGraph](../table-update-model.md).

[`ConstructSnapshot.callDataSnapshotFunction`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#callDataSnapshotFunction(java.lang.String,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotControl,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotFunction))
runs a snapshot. It attempts the operation without acquiring a lock, then verifies that the UpdateGraph did not change
any data while the operation was running. If data may have changed while the operation was running, the snapshotting
mechanism retries the operation.

If the snapshot cannot complete successfully after a limited number of attempts, or if a failed attempt runs longer
than a configured duration, the snapshotting mechanism acquires the shared lock to perform the operation reliably. The
[Configuration](#configuration) section lists the properties that set these limits.

Lock-free attempts don't always happen. [`callDataSnapshotFunction`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#callDataSnapshotFunction(java.lang.String,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotControl,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotFunction))
skips them, and runs the snapshot function once, in two cases:

- The calling thread already holds the shared or exclusive lock. Console commands hold the exclusive lock, so a
  snapshot that you take from the console runs this way.
- The calling thread is an UpdateGraph thread, one of the threads that the UpdateGraph uses to run update cycles and
  listeners.

### Obtaining snapshots

Obtaining a snapshot of tables or other data that the UpdateGraph updates takes two steps:

1. Create
   a [`SnapshotControl`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.SnapshotControl.html)
   object to control snapshot validation.
2. Run the snapshot by passing the [`SnapshotControl`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.SnapshotControl.html) and
   a [`SnapshotFunction`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.SnapshotFunction.html),
   which holds the logic that must run on consistent data, to [`callDataSnapshotFunction`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#callDataSnapshotFunction(java.lang.String,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotControl,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotFunction)).

#### Creating the SnapshotControl

To obtain snapshots, first use
[`ConstructSnapshot.makeSnapshotControl`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#makeSnapshotControl(boolean,boolean,io.deephaven.engine.table.impl.NotificationStepSource...))
to create a [`SnapshotControl`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.SnapshotControl.html) object, which tells the query engine how to verify that the snapshot result is correct.
The [`makeSnapshotControl`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#makeSnapshotControl(boolean,boolean,io.deephaven.engine.table.impl.NotificationStepSource...)) parameters `notificationAware`, `refreshing`, and `sources` control this verification process.

| Parameter Name      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `notificationAware` | Whether the snapshot result must capture every later notification from the sources. Set it to `true` when the snapshot initializes state that a [listener](../../how-to-guides/table-listeners-groovy.md) then updates from those notifications. In most other cases, use `false`.                                                                                                                                                           |
| `refreshing`        | Whether the [`SnapshotControl`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.SnapshotControl.html) needs to handle refreshing data. This must be `true` if any of the `sources` is [refreshing](https://deephaven.io/core/javadoc/io/deephaven/engine/updategraph/DynamicNode.html#isRefreshing()). When it is `false`, the snapshot function runs once, with no lock and no consistency check. |
| `sources`           | A varargs parameter that must contain every source the snapshot function reads, including every table.                                                                                                                                                                                                                                                                                                                                       |

#### Running a snapshot

To run a snapshot, pass three arguments to [`callDataSnapshotFunction`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#callDataSnapshotFunction(java.lang.String,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotControl,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotFunction)):

- A log prefix.
- The [`SnapshotControl`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.SnapshotControl.html).
- The [snapshot function](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.SnapshotFunction.html), typically a lambda. It may run more than once, as described in [Snapshotting](#snapshotting).

The snapshot function receives two arguments, `usePrev` and `beforeClockValue`. `usePrev` says which values to read:

- `true`: the snapshot is running during an update cycle before the sources finish updating, so the function must read [previous values](../table-update-model.md#removed-or-modified-values), the values as of the start of the current cycle.
- `false`: the function reads current values.

`beforeClockValue` is the [logical clock](https://deephaven.io/core/javadoc/io/deephaven/engine/updategraph/LogicalClock.html) value that the snapshotting mechanism read from the UpdateGraph before the attempt started. The logical clock changes when each update cycle starts and ends. The examples on this page don't use `beforeClockValue`.

Typically, a snapshot function retrieves data from several
[`ColumnSource`s](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html) for one or more
[row keys](../table-update-model.md), the keys that identify each row in a table. Consider the example
below, which retrieves data at row key `0` from two tables:

<!-- skip-test: getMyTable() and getMyOtherTable() stand in for the reader's own tables. -->

```groovy skip-test
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;

import java.util.concurrent.atomic.AtomicReference;

// Get tables to retrieve data from during the SnapshotFunction:
Table myTable = getMyTable();
Table myTable2 = getMyOtherTable();

// Obtain references to the desired ColumnSources from each table
ColumnSource<String> firstTableStrColSource = myTable.getColumnSource("MyStrCol", String.class);
ColumnSource<Integer> firstTableIntColSource = myTable.getColumnSource("MyIntCol", int.class);

ColumnSource<Double> secondTableDoubleColSource = myTable2.getColumnSource("MyDoubleCol", double.class);


// Create the SnapshotControl object:

// Most snapshots do not need to be 'notification aware'.
// The SnapshotControl needs to be 'notification aware' if the result of the snapshot is used to
// build state that is intended to be updated in response to subsequent notifications (e.g. from a
// listener).
boolean isNotificationAware = false;

// Determine whether the SnapshotControl needs to handle refreshing data:
boolean isRefreshing = myTable.isRefreshing() || myTable2.isRefreshing();

// It is *essential* that all tables referenced by the snapshot function (e.g. myTable and myTable2)
// are included in the 'sources' parameter of `makeSnapshotControl`.
ConstructSnapshot.SnapshotControl snapshotControl = ConstructSnapshot.makeSnapshotControl(
  isNotificationAware,
  isRefreshing,
  (NotificationStepSource) myTable, (NotificationStepSource) myTable2
);

// Create a class to hold the results:
class DataHolder {
  final String myStr;
  final int myInt;
  final double myDouble;

  DataHolder(String myStr, int myInt, double myDouble) {
    this.myStr = myStr;
    this.myInt = myInt;
    this.myDouble = myDouble;
  }
}

// Create a reference in which the snapshot function can store its results:
AtomicReference<DataHolder> snapshotResult = new AtomicReference<>();
ConstructSnapshot.callDataSnapshotFunction("my_snapshot", snapshotControl, (boolean usePrev, long beforeClockValue) -> {
  // For this example, we'll retrieve data from row key 0.
  final long idxKey = 0L;

  // Get the 'previous value' if usePrev is true. This occurs when the snapshot runs during an update
  // cycle before the sources have finished updating, so the 'current value' may still change.
  String myStringFromTable = usePrev ? firstTableStrColSource.getPrev(idxKey) : firstTableStrColSource.get(idxKey);
  int myIntFromTable = usePrev ? firstTableIntColSource.getPrevInt(idxKey) : firstTableIntColSource.getInt(idxKey);
  double myDoubleFromTable = usePrev ? secondTableDoubleColSource.getPrevDouble(idxKey) : secondTableDoubleColSource.getDouble(idxKey);

  // Create the result value and store it in the reference.
  // Since the snapshotResult reference is only used by one thread, it is better to use .setPlain() instead
  // of .set() to avoid the small additional cost of a volatile write.
  snapshotResult.setPlain(new DataHolder(myStringFromTable, myIntFromTable, myDoubleFromTable));

  // return true, indicating that the snapshot has succeeded
  return true;
});

// Since callDataSnapshotFunction() has returned and did not throw an exception,
// proceed with reading the snapshot result:
System.out.println("String from idx 0 of table: " + snapshotResult.getPlain().myStr);
```

For each attempt, [`callDataSnapshotFunction`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#callDataSnapshotFunction(java.lang.String,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotControl,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotFunction)) handles the result as follows:

- If the function throws a [`SnapshotUnsuccessfulException`](https://deephaven.io/core/javadoc/io/deephaven/engine/exceptions/SnapshotUnsuccessfulException.html) or a [`CancellationException`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/CancellationException.html), [`callDataSnapshotFunction`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#callDataSnapshotFunction(java.lang.String,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotControl,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotFunction)) rethrows it immediately without checking consistency.
- If the function throws a [`ConstructSnapshot.NoSnapshotAllowedException`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.NoSnapshotAllowedException.html), [`callDataSnapshotFunction`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#callDataSnapshotFunction(java.lang.String,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotControl,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotFunction)) makes no further lock-free attempts and runs the function again under the shared lock.
- After any other attempt that runs without the lock, the [`SnapshotControl`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.SnapshotControl.html) checks whether the data the function read stayed consistent. If the data was inconsistent, [`callDataSnapshotFunction`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#callDataSnapshotFunction(java.lang.String,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotControl,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotFunction)) discards the attempt, including any exception the function threw because it read inconsistent data, and runs the snapshot function again.
- After the attempts reach the limits in [Configuration](#configuration), [`callDataSnapshotFunction`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#callDataSnapshotFunction(java.lang.String,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotControl,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotFunction)) runs the function under the shared lock.
- If the data was consistent and the function returned `true`, the snapshot succeeds.
- If the data was consistent but the function returned `false` or threw an exception, [`callDataSnapshotFunction`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#callDataSnapshotFunction(java.lang.String,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotControl,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotFunction)) throws a [`SnapshotUnsuccessfulException`](https://deephaven.io/core/javadoc/io/deephaven/engine/exceptions/SnapshotUnsuccessfulException.html), with any exception from the function as its cause.
- When the function runs outside the lock-free attempts, returning `false` or throwing an exception makes [`callDataSnapshotFunction`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#callDataSnapshotFunction(java.lang.String,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotControl,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotFunction)) throw an exception.

### Early stopping of inconsistent snapshots

A snapshot function that runs several operations can stop early when the data becomes inconsistent. Between
operations, call
[`ConstructSnapshot.concurrentAttemptInconsistent`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#concurrentAttemptInconsistent()).
It returns `true` if the sources or the UpdateGraph's [logical clock](https://deephaven.io/core/javadoc/io/deephaven/engine/updategraph/LogicalClock.html) have changed in a way that makes
the current lock-free attempt inconsistent. It always returns `false` outside a lock-free attempt. When it returns
`true`, return `false` from the snapshot function to end the attempt early. [`callDataSnapshotFunction`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#callDataSnapshotFunction(java.lang.String,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotControl,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotFunction))
then finds the data inconsistent and runs the function again.

In the following example, the snapshot function retrieves data from 100 columns in each of two tables and checks for
consistency before it reads the second table:

<!-- skip-test: getMyTable(), getMyOtherTable(), and the elided columns stand in for the reader's own tables. -->

```groovy skip-test
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;

// Get the source tables:
final Table myTable = getMyTable();
final Table myTable2 = getMyOtherTable();

// Retrieve column sources:
final ColumnSource<String> table1ColSource1 = myTable.getColumnSource("Col1");
final ColumnSource<String> table1ColSource2 = myTable.getColumnSource("Col2");
// . . .
final ColumnSource<String> table1ColSource100 = myTable.getColumnSource("Col100");

final ColumnSource<String> table2ColSource1 = myTable2.getColumnSource("Col1");
final ColumnSource<String> table2ColSource2 = myTable2.getColumnSource("Col2");
// . . .
final ColumnSource<String> table2ColSource100 = myTable2.getColumnSource("Col100");

// Create the snapshotControl:
ConstructSnapshot.SnapshotControl snapshotControl = ConstructSnapshot.makeSnapshotControl(
  false,
  true,
  (NotificationStepSource) myTable, (NotificationStepSource) myTable2
);

// For this example, we'll retrieve data from row key 0.
final long idxKey = 0L;

// Create the snapshot result reference:
final String[] snapshotResults = new String[200];

// Run the snapshot:
ConstructSnapshot.callDataSnapshotFunction("my_snapshot", snapshotControl, (boolean usePrev, long beforeClockValue) -> {

  snapshotResults[0] = usePrev ? table1ColSource1.getPrev(idxKey) : table1ColSource1.get(idxKey);
  snapshotResults[1] = usePrev ? table1ColSource2.getPrev(idxKey) : table1ColSource2.get(idxKey);
  // . . .
  snapshotResults[99] = usePrev ? table1ColSource100.getPrev(idxKey) : table1ColSource100.get(idxKey);

  // Verify that the snapshot is consistent so far before retrieving the next batch of data.
  if (ConstructSnapshot.concurrentAttemptInconsistent()) {
    // Return false if the snapshot is already inconsistent
    return false;
  }

  snapshotResults[100] = usePrev ? table2ColSource1.getPrev(idxKey) : table2ColSource1.get(idxKey);
  snapshotResults[101] = usePrev ? table2ColSource2.getPrev(idxKey) : table2ColSource2.get(idxKey);
  // . . .
  snapshotResults[199] = usePrev ? table2ColSource100.getPrev(idxKey) : table2ColSource100.get(idxKey);


  // Return true; let the SnapshotControl verify the snapshot
  return true;
});

System.out.println(Arrays.toString(snapshotResults));
```

## Configuration

The following properties limit how many lock-free attempts a snapshot makes, and how long a failed attempt can run,
before the snapshotting mechanism acquires the shared lock.

| Property                                               | Default Value | Description                                                                                                                                     |
| ------------------------------------------------------ | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `ConstructSnapshot.maxConcurrentAttempts`              | 2             | Maximum number of lock-free snapshot attempts before acquiring the shared lock.                                                                 |
| `ConstructSnapshot.maxConcurrentAttemptDurationMillis` | 5000          | Maximum duration, in milliseconds, of a failed lock-free attempt before the snapshotting mechanism stops retrying and acquires the shared lock. |

## Related documentation

- [Deephaven's live Directed-Acyclic-Graph (DAG)](../dag.md)
- [Execution Context](../execution-context.md)
- [Incremental update model](../table-update-model.md)
- [Parallelizing queries](./parallelization.md)
- [Periodic Update Graph](../periodic-update-graph-configuration.md)
- [Initialization and updates](../../how-to-guides/initialization-and-updates.md)
- [Listen to ticking tables](../../how-to-guides/table-listeners-groovy.md)
- [Create a time table](../../how-to-guides/time-table.md)
- [Application Mode configuration](../../reference/app-mode/application-mode-config.md)
- [`awaitUpdate`](../../reference/table-operations/table-listeners/await-update.md)
- [`DynamicTableWriter`](../../reference/table-operations/create/DynamicTableWriter.md)
- [`naturalJoin`](../../reference/table-operations/join/natural-join.md)
- [`now`](../../reference/time/datetime/now.md)
- [`snapshot`](../../reference/table-operations/snapshot/snapshot.md)
- [`sort`](../../reference/table-operations/sort/sort.md)
- [`timeTable`](../../reference/table-operations/create/timeTable.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`where`](../../reference/table-operations/filter/where.md)
- [`ConstructSnapshot` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html)
- [`FunctionalLock` Javadoc](/core/javadoc/io/deephaven/util/locks/FunctionalLock.html)
- [`UpdateGraph` Javadoc](/core/javadoc/io/deephaven/engine/updategraph/UpdateGraph.html)
