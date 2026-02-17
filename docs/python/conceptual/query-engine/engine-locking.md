---
title: "Multithreading: Synchronization, locks, and snapshots"
sidebar_label: Synchronization and locking
---

Deephaven is often run on multiprocessor systems. Synchronized access to changing data from multiple threads is
implemented in the API with a **shared lock** for reading data, an **exclusive lock** for writing data and propagating
changes, and an [optimistic](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) **snapshotting** mechanism
to avoid locking while retrieving data and running table operations.

> [!NOTE]
> While the synchronization primitives presented in this guide are universal to Deephaven, the examples below use Deephaven's Java API instead of Python.

## Query engine locks

Synchronization of access to Deephaven data is managed by
the [Update Graph's](../../conceptual/table-update-model.md) [shared lock](https://deephaven.io/core/javadoc/io/deephaven/engine/updategraph/UpdateGraph.html#sharedLock())
and [exclusive lock](https://deephaven.io/core/javadoc/io/deephaven/engine/updategraph/UpdateGraph.html#exclusiveLock()).

Locks should be acquired only when necessary and held for the shortest duration possible, as the Update Graph
Processor cannot start an update cycle until the locks have been released by all non-UG threads.

As of Deephaven version 0.17.0, the shared and exclusive locks are backed by a
[`ReentrantReadWriteLock`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/locks/ReentrantReadWriteLock.html),
though this is subject to change.

### Shared lock

The **shared lock** is typically held while running table operations ([`where`](../../reference/table-operations/filter/where.md), [`natural_join`](../../reference/table-operations/join/natural-join.md), etc.) from independent threads, outside the Deephaven console and [app mode](../../reference/app-mode/application-mode-config.md) startup scripts. It can also be used when reading copious amounts of data from tables, but it is usually preferable to use [snapshotting](#snapshotting) in these cases to avoid the need to acquire the shared lock.

Any threads waiting for shared lock while the Periodic Update Graph is running will acquire it once the UG completes
its cycle and releases the exclusive lock. Any thread can immediately acquire the shared lock while the Update Graph
Processor is not running. However, the Periodic Update Graph will not be able to start a new refresh cycle while
any thread holds the shared lock.

### Exclusive lock

The **exclusive lock** is held by the query engine while processing updates. Only one thread at a time can hold the
exclusive lock, and no threads will hold the shared lock while the exclusive lock is locked. The exclusive lock is also
always held automatically when running commands in the Deephaven console or executing
an [app mode](../../reference/app-mode/application-mode-config.md) startup script.

#### Waiting for changes from the Periodic Update Graph

Users do not typically interact with this lock directly, but in certain cases it is used when waiting for changes from
the Periodic Update Graph.

Most commonly, when waiting for table updates (using [`awaitUpdate()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#awaitUpdate())), the exclusive lock is acquired automatically. This is required if a Periodic Update Graph cycle must occur in the middle of another operation — for example, if data is flushed to a [DynamicTableWriter](../../reference/table-operations/create/DynamicTableWriter.md) in the middle of query initialization, it will not be reflected in the output table until the Periodic Update Graph runs a refresh cycle.

> [!NOTE]
> Upgrading from the shared lock to the exclusive lock is not supported and will throw an exception.
> Accordingly, `awaitUpdate()` cannot be used while shared lock is held.

In advanced cases where `awaitUpdate()` is insufficient, the exclusive lock can also be held in order to wait on [conditions](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/locks/Condition.html) from the UG. This is helpful in situations where an independent (non-UG) thread must wait for a change or notification propagated by the Periodic Update Graph, often in conjunction with a custom [listener](../../how-to-guides/table-listeners-python.md). In this situation, a condition can be obtained using `ExecutionContext.getContext().getUpdateGraph().exclusiveLock().newCondition()`, the independent thread can [`await()`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/locks/Condition.html#await()) the condition, and the can condition can be signaled by calling [`requestSignal()`](https://deephaven.io/core/javadoc/io/deephaven/engine/updategraph/UpdateGraph.html#requestSignal(java.util.concurrent.locks.Condition)) from the Periodic Update Graph's refresh thread. A simple example of this pattern that can be run in Groovy is
provided below:

```groovy skip-test
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.context.ExecutionContext;

import java.util.concurrent.locks.Condition;

final Condition myCondition = ExecutionContext.getContext().getUpdateGraph().exclusiveLock().newCondition();

Thread independentThread = new Thread(() -> {
  System.out.println(Thread.currentThread().getName() + " - " + currentTime() + " - Acquiring update graph exclusive lock...");
    ExecutionContext.getContext().getUpdateGraph().exclusiveLock().doLocked(() -> {
    System.out.println(Thread.currentThread().getName() + " - " + currentTime() + " - Waiting for condition...");
    myCondition.await();
    System.out.println(Thread.currentThread().getName() + " - " + currentTime() + " - Condition was signaled!");
  });
  System.out.println(Thread.currentThread().getName() + " - " + currentTime() + " - update graph exclusive lock released.")

}, "myIndependentThread");
independentThread.start();

// Create a time table that ticks every 5 seconds
myTable = timeTable("00:00:05");

myListener = new InstrumentedTableUpdateListenerAdapter(myTable, false) {
  @Override
  public void onUpdate(TableUpdate upstream) {
    // Request that the UpdateGraph signal the condition, so that anything
    // await()ing the condition is notified.
    System.out.println(Thread.currentThread().getName() + " - " + currentTime() + " - Signaling condition...")
      ExecutionContext.getContext().getUpdateGraph().requestSignal(myCondition);
    System.out.println(Thread.currentThread().getName() + " - " + currentTime() + " - Condition signaled.")
  }
};

myTable.addUpdateListener(myListener);
```

The output to demonstrates the progress of the two threads. First, `myIndependentThread` acquires the lock and
waits for the condition. Shortly afterward, the Periodic Update Graph processes a new `timeTable` tick on its
five-second interval — triggering the listener's `onUpdate()` method, which signals the condition. After the condition
has been signaled and the Periodic Update Graph finishes its cycle, `myIndependentThread` returns from
`myCondition.await()` and resumes execution. The Periodic Update Graph will continue to run the listener until the
listener is either [removed](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#removeUpdateListener(io.deephaven.engine.table.TableUpdateListener))
from the table or goes out of scope and is removed by JVM garbage collection.

```
myIndependentThread - 2022-10-12T12:36:06.890861000 NY - Acquiring update graph exclusive lock...
myIndependentThread - 2022-10-12T12:36:06.998700000 NY - Waiting for condition...
PeriodicUpdateGraph.DEFAULT.refreshThread - 2022-10-12T12:36:07.142583000 NY - Signaling condition...
PeriodicUpdateGraph.DEFAULT.refreshThread - 2022-10-12T12:36:07.150394000 NY - Condition signaled.
myIndependentThread - 2022-10-12T12:36:07.153525000 NY - Condition was signaled!
myIndependentThread - 2022-10-12T12:36:07.154153000 NY - Update graph exclusive lock released.
PeriodicUpdateGraph.DEFAULT.refreshThread - 2022-10-12T12:36:10.117031000 NY - Signaling condition...
PeriodicUpdateGraph.DEFAULT.refreshThread - 2022-10-12T12:36:10.117912000 NY - Condition signaled.
PeriodicUpdateGraph.DEFAULT.refreshThread - 2022-10-12T12:36:15.115583000 NY - Signaling condition...
PeriodicUpdateGraph.DEFAULT.refreshThread - 2022-10-12T12:36:15.116385000 NY - Condition signaled.
```

### Using locks

The preferred way to use either lock is as a
[functional lock](/core/javadoc/io/deephaven/util/locks/FunctionalLock.html), where the operation
to execute (the [`SnapshotFunction`](/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.SnapshotFunction.html))
is passed as an argument (typically a lambda expression). This simplifies the standard `try`/`finally`
locking pattern (demonstrated under [Explicit locking and unlocking](#explicit-locking-and-unlocking))

The `doLocked()` method allows for arbitrary code to be executed while holding the lock:

```groovy skip-test
Table myTable = getMyTable();
ExecutionContext.getContext().getUpdateGraph().sharedLock().doLocked( () -> {
  // Retrieve the data from "MyCol" as an array and print it:
  String[] myColAsArray = myTable.getColumn("MyCol").getDirect()
  System.out.println(Arrays.toString(col1array));
});
```

It is also possible to compute a value while holding the lock and return it directly to the caller,
using `computeLocked()`:

```groovy skip-test
Table myTable = getMyTable();
String[] myColAsArray = ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked( () -> {
  // Retrieve the data from "MyCol" as an array and return it:
  return myTable.getColumn("MyCol").getDirect();
});
```

Both `doLocked()` and `computeLocked()` allow the supplier to throw [checked exceptions](https://docs.oracle.com/javase/specs/jls/se7/html/jls-11.html#jls-11.2.3)
(e.g. `IOException`, `TimeoutException`, or anything else that is not a subclass of `RuntimeException`). In these cases,
the caller must handle the potential exception.

```groovy skip-test
try {
  ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(() -> { throw new IOException(); });
} catch (IOException e) {
  throw new UncheckedIOException("IOException occurred during computeLocked()", e);
}
```

##### Explicit locking and unlocking

Although it is not recommended, it is also possible to acquire and release a lock explicitly using the `lock()` and
`unlock()` methods. When doing so, you must take care to ensure that the lock is always released in a `finally` block,
so that it is guaranteed to be released even when exceptions or errors occur. (The `doLocked()` and `computeLocked()`
methods handle this automatically.)

Below is an example of the correct way to use the shared lock with explicit locking.

```groovy skip-test
// Acquire the shared lock
ExecutionContext.getContext().getUpdateGraph().sharedLock().lock();
try {
  // code to run under the lock
} finally {
  ExecutionContext.getContext().getUpdateGraph().sharedLock().unlock();
}
```

## Snapshotting

A **snapshot** is a guaranteed-consistent result derived from data in Deephaven without necessarily acquiring a lock.
Deephaven's snapshotting mechanism is a form of
[optimistic concurrency](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) that helps to retrieve data
safely, quickly, and without blocking the Periodic Update Graph.

Snapshots are performed
using [`ConstructSnapshot.callDataSnapshotFunction()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#callDataSnapshotFunction(java.lang.String,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotControl,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotFunction)),
which will attempt the operation without acquiring a lock, then verify that the Update Graph did not change any data
while the operation was running. If data may have changed while the operation was running, the snapshotting mechanism
will retry the operation.

If the snapshot cannot complete successfully after a limited number of attempts, or a limited duration of wall-clock
time, the snapshotting mechanism will automatically acquire the shared lock in order to reliably perform the operation.
The maximum number and duration of these attempts before acquiring the lock is controlled by the following properties.

| Property                                               | Default Value | Description                                    |
| ------------------------------------------------------ | ------------- | ---------------------------------------------- |
| `ConstructSnapshot.maxConcurrentAttempts`              | 2             | The number of attempts at complete             |
| `ConstructSnapshot.maxConcurrentAttemptDurationMillis` | 5000          | The maximum amount of time to try snapshotting |

### Obtaining snapshots

There are two steps to obtaining a snapshot of tables (or other data updated by the Periodic Update Graph):

1. Create
   a [`SnapshotControl`](/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.SnapshotControl.html)
   object to control snapshot validation.
2. Run
   a [`SnapshotFunction`](/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.SnapshotFunction.html)
   that encapsulates the logic that must be run on consistent data.

#### Creating the SnapshotControl

To obtain snapshots, first use
[`ConstructSnapshot.makeSnapshotControl()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#makeSnapshotControl(boolean,boolean,io.deephaven.engine.table.impl.NotificationStepSource))
to create a `SnapshotControl` object, which tells the query engine how to verify that the snapshot result is correct.
The `makeSnapshotControl()` parameters `isNotificationAware`, `isRefreshing` and `sources` control this verification process.

| Parameter Name        | Description                                                                                                                                                                                                                                  |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `isNotificationAware` | Whether the snapshot control. In most cases this should be `false`. If the snapshot is used to initialize a component that is subsequently updated in response to additional notifications (e.g. by a listener), then this should be `true`. |
| `isRefreshing`        | Whether the snapshot control needs to handle refreshing data. This must be `true` if any of the `sources` is [refreshing](https://deephaven.io/core/javadoc/io/deephaven/engine/updategraph/DynamicNode.html#isRefreshing()).                |
| `sources`             | A varargs parameter that should contain all sources (including tables) that will be read by the snapshot function.                                                                                                                           |

#### Running a snapshot

To run a snapshot, pass a log prefix, the snapshot control object, and the
[snapshot function](/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.SnapshotFunction.html)
itself (typically a lambda) to
[`callDataSnapshotFunction()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#callDataSnapshotFunction(java.lang.String,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotControl,io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotFunction))
to begin the snapshot process. The snapshot function may be called repeatedly, with or without the lock, depending on
the configured limits on concurrent attempts. The `usePrev` parameter informs the snapshot function of whether
it should use values from the current cycle or the previous cycle — if the UpdateGraph is running when the
snapshot function is called, `usePrev` will be `true`; otherwise it is `false`.

Typically, a snapshot function will retrieve data from several [ColumnSources](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)
for one or more index keys. Consider the example below, which retrieves data at index key `0` from two tables:

```groovy skip-test
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;

// Get tables to retrieve data from during the SnapshotFunction:
Table myTable = getMyTable();
Table myTable2 = getMyOtherTable();

// Obtain references to the desired ColumnSources from each table
ColumnSource<String> firstTableStrColSource = myTable.getColumnSource("MyStrCol", String.class);
ColumnSource<Integer> firstTableIntColSource = myTable.getColumnSource("MyIntCol", Integer.class);

ColumnSource<Double> secondTableDoubleColSource = myTable.getColumnSource("MyIntCol", Double.class);


// Create the SnapshotControl object:

// Most snapshots do not need to be 'notification aware'. A snapshot
// The SnapshotControl needs to be 'notification aware' if the result of the snapshot is used to
// build state that is intended to be updated in response to subsequent notifications (e.g. from a
// listener).
boolean isNotificationAware = false;

// Determine whether the SnapshotControl needs to handle refreshing data:
boolean isRefreshing = myTable.isRefreshing() || myTable2.isRefreshing();

// It is *essential* that all tables referenced by the snapshot function (e.g. myTable and myTable2)
// are included in the 'sources' parameter of `makeSnapshotControl()`.
ConstructSnapshot.SnapshotControl snapshotControl = ConstructSnapshot.makeSnapshotControl(
  isNotificationAware,
  isRefreshing,
  myTable, myTable2
)

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
  // For this example, we'll retrieve data from index key 0.
  final long idxKey = 0L;

  // Get the 'previous value' if usePrev is true. This occurs when the UpdateGraph is running to update
  // the 'current value' with the latest data.
  String myStringFromTable = usePrev ? firstTableStrColSource.getPrev(idxKey) : firstTableStrColSource.get(idxKey);
  int myIntFromTable = usePrev ? firstTableIntColSource.getPrevInt(idxKey) : firstTableIntColSource.getInt(idxKey);
  double myDoubleFromTable = usePrev ? secondTableDoubleColSource.getPrevDouble(idxKey) : secondTableIntColSource.getDouble(idxKey);

  // Create the result value and store it in the reference.
  // Since the snapshotResult reference is only used by one thread, it is better to use .setPlain() instead
  // of .set() to avoid the small additional cost of a volatile write.
  snapshotResult.setPlain(new DataHolder(myStringFromTable, myIntFromTable, myDoubleFromTable));

  // return true, indicating that the snapshot has succeeded
  return true;
});

// Since callDataSnapshotFunction() has returned and did not throw an exception,
// proceed with reading the snapshot result:
System.out.println("String from idx 0 of table: " + snapshotResult.getPlain());
```

If the snapshot function returns `false`, it is assumed that the data was inconsistent and the snapshot function is
rerun. If the snapshot function completes by returning `true` or throwing an exception, the `SnapshotControl` will be
used to validate that the data used by the snapshot is consistent — if not, the snapshot function is rerun. (The
exceptions are only propagated by `callDataSnapshotFunction()` when validation succeeds, since it is expected that
exceptions may occur when the data is inconsistent.) If the snapshot function throws an exception or returns `false`
after `callDataSnapshotFunction()` has acquired the shared lock, then `callDataSnapshotFunction()` will throw
an exception.

#### Early stopping of inconsistent snapshots

For snapshot functions comprising multiple operations, it is possible to check mid-operation whether the data has become
inconsistent and determine early that the snapshot will not be reliable and should be aborted. This can be done by
calling [`ConstructSnapshot.concurrentAttemptInconsistent()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/remote/ConstructSnapshot.html#concurrentAttemptInconsistent())
to verify that UpdateGraph's logical clock value has not changed since the snapshot function was called. If the
clock has ticked, the snapshot function can return `false` to immediately abort and retry.

Consider the example below, in which data is retrieved from hundreds of columns.

```groovy skip-test
// Get the source tables:
final Table myTable = getMyTable();
final Table myTable2 = getMyOtherTable();

// Retrieve column sources:
final ColumnSource<String> table1colSource1 = myTable.getColumnSource("Col1");
final ColumnSource<String> table1colSource2 = myTable.getColumnSource("Col2");
// . . .
final ColumnSource<String> table1colSource100 = myTable.getColumnSource("Col100");

final ColumnSource<String> table2colSource1 = myTable2.getColumnSource("Col1");
final ColumnSource<String> table2colSource2 = myTable2.getColumnSource("Col2");
// . . .
final ColumnSource<String> table2colSource100 = myTable2.getColumnSource("Col100");

// Create the snapshotControl:
ConstructSnapshot.SnapshotControl snapshotControl = ConstructSnapshot.makeSnapshotControl(
  false,
  true,
  myTable, myTable2
);

// Create the snapshot result reference:
final String[] snapshotResults = new String[200];

// Run the snapshot:
ConstructSnapshot.callDataSnapshotFunction("my_snapshot", snapshotControl, (boolean usePrev, long beforeClockValue) -> {

  snapshotResults[0] = usePrev ? table1colSource1.getPrev(idxKey) : table1colSource1.get(idxKey);
  snapshotResults[1] = usePrev ? table1colSource2.getPrev(idxKey) : table1colSource2.get(idxKey);
  // . . .
  snapshotResults[99] = usePrev ? table1colSource100.getPrev(idxKey) : table1colSource100.get(idxKey);

  // Verify that the snapshot is consistent so far before retrieving the next batch of data.
  if (ConstructSnapshot.concurrentAttemptInconsistent()) {
    // Return false if the snapshot is already inconsistent
    return false;
  }

  snapshotResults[100] usePrev ? table2colSource1.getPrev(idxKey) : table2colSource1.get(idxKey);
  snapshotResults[101] usePrev ? table2colSource2.getPrev(idxKey) : table2colSource2.get(idxKey);
  // . . .
  snapshotResults[199] = usePrev ? table2colSource100.getPrev(idxKey) : table2colSource100.get(idxKey);


  // Return true; let the SnapshotControl verify the snapshot
  return true;
})

Table snapshotResult = snapshotResultRef.getPlain();
```

## Choosing between snapshots and locks

While the right choice among snapshots and locks depends heavily on the operation to be run and the context it is run
from, the following guidelines can be helpful in making the choice:

- When running code in the Deephaven console, it's rarely necessary to worry about any of these mechanisms — the exclusive lock is automatically held when running code in the console.
- Use **_snapshots_** for filters and sorts in service of a user interface. (The Deephaven UI itself uses snapshotting to maximize responsiveness.) Snapshots can be processed even while the Update Graph is updating, and in some cases they return considerably more quickly than a lock could be acquired.
- Use the **_shared lock_** when running table operations from a context that does not already hold a lock (e.g. arbitrary threads). Most table operations (beyond filters and sorts) cannot be initialized within a snapshot and must be run under a lock, and the shared lock is preferable to the exclusive lock because it allows multiple threads to run concurrently.
- Use the **_exclusive lock_** in advanced cases that must wait on processing performed by the Periodic Update Graph's refresh thread and are not served by [`Table.awaitUpdate()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#awaitUpdate()).

### Related documentation

- [Deephaven’s Directed-Acyclic-Graph (DAG)](../dag.md)
- [Parallelizing queries](./parallelization.md)
