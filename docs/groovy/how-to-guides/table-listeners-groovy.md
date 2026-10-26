---
title: Listen to ticking tables
---

Deephaven queries update in real time. When a table updates, Deephaven sends a message describing the changes to each of the table's listeners. This mechanism makes ticking queries work, and you can use it to build your own dynamic functionality.

As an example, consider using a Deephaven query to create a dynamic table that monitors for situations needing human intervention. You can create a table listener that sends a Slack message every time one or more tables tick. Similarly, you could have a table of orders to buy or sell stocks. When rows are added to the order table, a listener sends new orders to the broker, and when rows are removed, it cancels those orders with the broker.

This guide shows you how to create your own table listeners in Groovy.

## What is a table listener?

A table listener is an object that listens to one or more tables for updates. When connected to a ticking table, a listener receives one or more [`TableUpdate`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html) objects, which you use to access the added, modified, or removed data.

Deephaven processes changes to ticking tables in batches called update cycles. In each update cycle, a listener receives one [`TableUpdate`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html) for each of its tables that changed. Each `TableUpdate` describes the rows added, modified, and removed during that cycle. For details, see the [table update model](../conceptual/table-update-model.md).

When listening to changes in a single table, extend [`InstrumentedTableUpdateListenerAdapter`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html), and when listening to changes in multiple tables, extend [`MergedListener`](/core/javadoc/io/deephaven/engine/table/impl/MergedListener.html).

## Listen to one ticking table

To listen to a table, create an instance of [`InstrumentedTableUpdateListenerAdapter`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html) and add it to the table with the [`Table.addUpdateListener(listener)`](/core/javadoc/io/deephaven/engine/table/Table.html#addUpdateListener(io.deephaven.engine.table.TableUpdateListener)) method. Once a listener is registered, it begins receiving updates. To control what the listener does upon receiving an update, override the listener's [`onUpdate`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html#onUpdate(io.deephaven.engine.table.TableUpdate)) method.

In this example, the listener keeps track of how many times it has been called.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

t1 = timeTable("PT1S").update("X = i").tail(5)

h1 = new InstrumentedTableUpdateListenerAdapter(t1, false) {
    def counter = 0

    @Override
    public void onUpdate(TableUpdate upstream) {
        println "LISTENER: counter=${++counter} update=${upstream}"
    }
}

t1.addUpdateListener(h1)
```

![`t1` updates while `h1` prints updates to the log](../assets/how-to/listener-one-table.gif)

The constructor takes the table to listen to and a `retain` flag. Set `retain` to `true` to keep the listener from being garbage collected even if no variable references it. An optional first argument provides a description, as in later examples on this page.

## Listen to multiple ticking tables

To listen to more than one table at once, use a [`MergedListener`](/core/javadoc/io/deephaven/engine/table/impl/MergedListener.html). Once a listener is registered, it begins receiving updates.

The [`MergedListener`](/core/javadoc/io/deephaven/engine/table/impl/MergedListener.html) constructor takes four arguments:

- `recorders`: An iterable set of [`ListenerRecorder`](/core/javadoc/io/deephaven/engine/table/impl/ListenerRecorder.html) instances.
- `dependencies`: Tables the listener waits on before it runs in each update cycle, so it can read them safely. See [Dependent tables](#dependent-tables).
- `listenerDescription`: A `String` description of the listener.
- `result`: A result table that uses the listener's tables as sources, or `null`.

Each [`ListenerRecorder`](/core/javadoc/io/deephaven/engine/table/impl/ListenerRecorder.html) listens to one table and records its update. Its constructor takes a description, the table to listen to, and an optional object that keeps the recorder alive. The example passes `null` for that object.

To register the merged listener, call each recorder's [`setMergedListener`](/core/javadoc/io/deephaven/engine/table/impl/ListenerRecorder.html#setMergedListener(io.deephaven.engine.table.impl.MergedListener)) method so it notifies the merged listener. Then add each recorder to its table with [`addUpdateListener`](/core/javadoc/io/deephaven/engine/table/Table.html#addUpdateListener(io.deephaven.engine.table.TableUpdateListener)).

The following example listens to two time tables, one ticking every second and the other ticking every two seconds, and passes an empty `dependencies` list. Each [`ListenerRecorder`](/core/javadoc/io/deephaven/engine/table/impl/ListenerRecorder.html)'s [`getUpdate`](/core/javadoc/io/deephaven/engine/table/impl/ListenerRecorder.html#getUpdate()) method returns a [`TableUpdate`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html) if its table changed during the update cycle, and `null` otherwise. The example overrides the [`MergedListener`](/core/javadoc/io/deephaven/engine/table/impl/MergedListener.html)'s [`process`](/core/javadoc/io/deephaven/engine/table/impl/MergedListener.html#process()) method to print each update it receives.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.ListenerRecorder
import io.deephaven.engine.table.impl.MergedListener

t1 = timeTable("PT1S").update("X = i").tail(5)
t2 = timeTable("PT2S").update("X = i * 2").tail(5)

recorder1 = new ListenerRecorder("t1", t1, null)
recorder2 = new ListenerRecorder("t2", t2, null)

h = new MergedListener([recorder1, recorder2], [], "Description", null) {
    def counter = 0

    @Override
    void process() {
        counter++
        TableUpdate tu1 = recorder1.getUpdate()
        TableUpdate tu2 = recorder2.getUpdate()
        if (tu1 != null) {
            println "MERGED LISTENER: counter=${counter} t1 update=${tu1}"
        }
        if (tu2 != null) {
            println "MERGED LISTENER: counter=${counter} t2 update=${tu2}"
        }
    }
}

recorder1.setMergedListener(h)
recorder2.setMergedListener(h)

t1.addUpdateListener(recorder1)
t2.addUpdateListener(recorder2)
```

![`t1` and `t2` update while `h` prints updates to the log](../assets/how-to/listener-merged.gif)

> [!IMPORTANT]
> The [`TableUpdate`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html) returned by [`getUpdate`](/core/javadoc/io/deephaven/engine/table/impl/ListenerRecorder.html#getUpdate()) is `null` for any table that has not changed during the update cycle. Check for `null` before using the update, or the listener throws a `NullPointerException`.

## Access table data

A [`TableUpdate`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html) object identifies the rows that changed (added, modified, and removed) by their [row keys](../conceptual/table-update-model.md#describing-table-updates). Unlike row positions, row keys usually stay the same when other rows are added or removed. When a row's key does change, the update records the change as a [shift](../conceptual/table-update-model.md#shifts).

The following methods return a [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) that identifies the added, removed, or modified rows:

- [`added`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html#added()): Rows added during the current update cycle.
- [`modified`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html#modified()): Rows modified during the current update cycle.
- [`removed`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html#removed()): Rows removed during the current update cycle.
- [`getModifiedPreShift`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html#getModifiedPreShift()): Modified rows, identified by the row keys they had at the start of the update cycle. A row's key can change during a cycle when rows are inserted or removed ahead of it. See [shifts](../conceptual/table-update-model.md#shifts) in the table update model.

These methods return row keys, not column values. To read a value, look up its row key in a [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html) with the accessor that matches the key:

- [`added`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html#added()) and [`modified`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html#modified()) return post-shift row keys, the keys rows have at the end of the update cycle. Read them with current-value accessors such as [`getInt`](/core/javadoc/io/deephaven/engine/table/ElementSource.html#getInt(long)).
- [`removed`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html#removed()) and [`getModifiedPreShift`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html#getModifiedPreShift()) return pre-shift row keys. Read them with previous-value accessors such as [`getPrevInt`](/core/javadoc/io/deephaven/engine/table/ElementSource.html#getPrevInt(long)).

The following example listens to added rows during each update cycle. It prints the data as the listener receives it.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.ColumnSource
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter
import io.deephaven.engine.rowset.RowSet
import java.time.Instant

t1 = timeTable("PT1S").update("X = 100 + i").tail(5)

h1 = new InstrumentedTableUpdateListenerAdapter(t1, false) {

    @Override
    public void onUpdate(TableUpdate upstream) {
        RowSet.Iterator iter = upstream.added().iterator()
        ColumnSource<Instant> colSrc1 = t1.getColumnSource("Timestamp", Instant.class)
        ColumnSource<Integer> colSrc2 = t1.getColumnSource("X", int.class)

        while (iter.hasNext()) {
            long rowKey = iter.next()
            Instant col1data = colSrc1.get(rowKey)
            Class<?> col1type = colSrc1.getType()
            int col2data = colSrc2.getInt(rowKey)
            Class<?> col2type = colSrc2.getType()
            println "{'Timestamp': [data=${col1data}, type=${col1type}], 'X': [data=${col2data}, type=${col2type}]}"
        }
    }
}

t1.addUpdateListener(h1)
```

![Log output from `h1`](../assets/how-to/listener-access.gif)

The following example listens to modified rows during each update cycle. It uses a [`RowSet.Iterator`](/core/javadoc/io/deephaven/engine/rowset/RowSet.Iterator.html) to print the current and previous values of the modified rows for the `X` column.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.ColumnSource
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter
import io.deephaven.engine.rowset.RowSet

t1 = timeTable("PT0.1S").update("X = i").lastBy()

h1 = new InstrumentedTableUpdateListenerAdapter("listener", t1, false) {
    @Override
    public void onUpdate(TableUpdate upstream) {
        RowSet prevModified = upstream.getModifiedPreShift()
        RowSet currModified = upstream.modified()
        if (prevModified.size() == 0) {
            println "No previous values"
            return
        }

        ColumnSource<Integer> xCol = t1.getColumnSource("X", int.class)

        RowSet.Iterator prevIt = prevModified.iterator()
        RowSet.Iterator currIt = currModified.iterator()

        while (prevIt.hasNext() && currIt.hasNext()) {
            long prevRowKey = prevIt.next()
            long currRowKey = currIt.next()
            int prev = xCol.getPrevInt(prevRowKey)
            int curr = xCol.getInt(currRowKey)
            println "Change previous=${prev} current=${curr}"
        }
    }
}

t1.addUpdateListener(h1)
```

![`t1` updates while `h1` prints modified rows](../assets/how-to/listener-mod-prev.gif)

> [!NOTE]
> A [`TableUpdate`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html) is normally valid only inside [`onUpdate`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html#onUpdate(io.deephaven.engine.table.TableUpdate)). If you pass an update to code that runs later in the same update cycle, call [`acquire`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html#acquire()) on it first and [`release`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html#release()) when that code is done. An acquired update is never valid after the cycle that created it ends.

## Error handling

If a listener encounters an error, it should throw an exception rather than catch and suppress it. When [`onUpdate`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html#onUpdate(io.deephaven.engine.table.TableUpdate)) throws an exception, or the table the listener listens to fails:

- Deephaven logs the error and marks the listener as failed.
- The failed listener stops receiving updates.
- Deephaven calls the listener's [`onFailureInternal`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html#onFailureInternal(java.lang.Throwable,io.deephaven.engine.table.TableListener.Entry)) method.

Override [`onFailureInternal`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html#onFailureInternal(java.lang.Throwable,io.deephaven.engine.table.TableListener.Entry)) to notify the user, client APIs, or other services, and call `super.onFailureInternal` to keep the default error reporting.

In the following example, the listener throws a [`RuntimeException`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/RuntimeException.html) when it receives a value greater than 10. The overridden [`onFailureInternal`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html#onFailureInternal(java.lang.Throwable,io.deephaven.engine.table.TableListener.Entry)) method prints a message and then calls `super.onFailureInternal`.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableListener
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

source = timeTable("PT1S").update("X = i + 6")

listener = new InstrumentedTableUpdateListenerAdapter("listener", source, false) {

    @Override
    void onUpdate(TableUpdate upstream) {
        def x = source.getColumnSource("X", int.class)

        upstream.added().forAllRowKeys { long rowKey ->
            if (x.getInt(rowKey) > 10) {
                throw new RuntimeException("Value over 10 detected!")
            }
        }
    }

    @Override
    void onFailureInternal(Throwable originalException, TableListener.Entry sourceEntry) {
        println "A value over 10 was detected. Please investigate."
        super.onFailureInternal(originalException, sourceEntry)
    }
}

source.addUpdateListener(listener)
```

![`source` updates while `listener` throws an exception when a value exceeds 10](../assets/how-to/listener-error.gif)

## Add and remove listeners

Most applications keep a table listener for their whole lifetime. To register a listener for only part of that time, remove it from the table with [`Table.removeUpdateListener(listener)`](/core/javadoc/io/deephaven/engine/table/Table.html#removeUpdateListener(io.deephaven.engine.table.TableUpdateListener)).

The following example uses [`java.util.Timer`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Timer.html) to remove a listener after 3 seconds and then add it back after 6 seconds. The timer callbacks run on a background thread. As a precaution, the callback that re-adds the listener holds the shared lock, so the listener is registered between update cycles. For more on locking from other threads, see [Update graph locks and thread safety](#update-graph-locks-and-thread-safety).

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

class ExampleListener extends InstrumentedTableUpdateListenerAdapter {

    ExampleListener(String description, Table source, boolean retain) {
        super(description, source, retain)
    }

    @Override
    void onUpdate(TableUpdate upstream) {
        println "LISTENER: update=${upstream}"
    }
}

// Console code is already protected — no lock needed here
t = timeTable("PT1S").update("X = i").tail(5)
listener = new ExampleListener("Test Listener", t, true)
println "Adding listener"
t.addUpdateListener(listener)

new Timer().runAfter(3000) {
    println "Removing listener"
    t.removeUpdateListener(listener)
}

new Timer().runAfter(6000) {
    // Timer runs on a background thread — acquire the lock
    t.getUpdateGraph().sharedLock().doLocked(() -> {
        println "Adding listener"
        t.addUpdateListener(listener)
    })
}
```

![A listener is added and removed](../assets/how-to/listener-lock.gif)

## Replay data

A table listener can receive the rows a table already holds when the listener is registered. For example, a listener that isn't registered until 10 seconds after a time table starts ticking can receive the rows the table added during those 10 seconds.

The built-in replay option works only with a shift-oblivious listener, such as [`ShiftObliviousInstrumentedListenerAdapter`](/core/javadoc/io/deephaven/engine/table/impl/ShiftObliviousInstrumentedListenerAdapter.html), not the [`InstrumentedTableUpdateListenerAdapter`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html) used elsewhere on this page. A shift-oblivious listener differs in two ways:

- It receives no row shift information.
- Its [`onUpdate`](/core/javadoc/io/deephaven/engine/table/ShiftObliviousListener.html#onUpdate(io.deephaven.engine.rowset.RowSet,io.deephaven.engine.rowset.RowSet,io.deephaven.engine.rowset.RowSet)) method receives the added, removed, and modified [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)s instead of a [`TableUpdate`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html).

Add the listener with the two-argument [`addUpdateListener`](/core/javadoc/io/deephaven/engine/table/Table.html#addUpdateListener(io.deephaven.engine.table.ShiftObliviousListener,boolean)) overload and set `replayInitialImage` to `true`. If the table is not empty, `addUpdateListener` immediately calls the listener's [`onUpdate`](/core/javadoc/io/deephaven/engine/table/ShiftObliviousListener.html#onUpdate(io.deephaven.engine.rowset.RowSet,io.deephaven.engine.rowset.RowSet,io.deephaven.engine.rowset.RowSet)) method with the existing rows as added rows.

The following example creates a time table:

```groovy ticking-table order=null test-set=replay reset
source = timeTable("PT0.3S").update("X = i")
```

After a few seconds, run the following code to register two listeners with the table. Only the one added with `replayInitialImage` set to `true` receives the rows the table already holds when it is registered.

```groovy ticking-table order=null test-set=replay reset
import io.deephaven.engine.table.impl.ShiftObliviousInstrumentedListenerAdapter

noReplayListener = new ShiftObliviousInstrumentedListenerAdapter("No replay listener", source, false) {
    @Override
    void onUpdate(RowSet added, RowSet removed, RowSet modified) {
        println "NO REPLAY LISTENER: added=${added}, removed=${removed}, modified=${modified}"
    }
}

replayListener = new ShiftObliviousInstrumentedListenerAdapter("Replay listener", source, false) {
    @Override
    void onUpdate(RowSet added, RowSet removed, RowSet modified) {
        println "REPLAY LISTENER: added=${added}, removed=${removed}, modified=${modified}"
    }
}

// Set replayInitialImage=false to receive only changes made after the listener is added
source.addUpdateListener(noReplayListener, false)

// Set replayInitialImage=true to replay data that existed in the table before the listener was added
source.addUpdateListener(replayListener, true)
```

![The listener added with `replayInitialImage=true` receives the rows the table already held, while the other listener only receives the updates after it was registered](../assets/how-to/listener-replay.gif)

When you no longer need the listeners, remove them:

```groovy order=null test-set=replay reset
source.removeUpdateListener(noReplayListener)
source.removeUpdateListener(replayListener)
```

## Dependent tables

A listener can read tables other than the one it listens to. Register those tables as dependencies of the listener, and the query engine waits to call the listener until it has processed all of them. A table that isn't a dependency may be in an inconsistent state when the listener reads it.

For example, `sourceA` and `sourceB` tick at the same time but share no key column, so instead of joining them, a listener on `sourceA` reads `sourceB` directly. Without a dependency, that listener might run before the engine finishes processing `sourceB`.

How you register a dependency depends on the listener class:

- For a [`MergedListener`](/core/javadoc/io/deephaven/engine/table/impl/MergedListener.html), pass the tables in its `dependencies` constructor argument, as described in [Listen to multiple ticking tables](#listen-to-multiple-ticking-tables).
- For an [`InstrumentedTableUpdateListenerAdapter`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html), override its [`canExecute`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html#canExecute(long)) method so it also waits for each dependency. A dependency's [`satisfied`](/core/javadoc/io/deephaven/engine/updategraph/NotificationQueue.Dependency.html#satisfied(long)) method returns `true` once it has finished processing the current update cycle.

The following example makes `sourceB` a dependency of a listener on `sourceA`, then reads `sourceB` directly in [`onUpdate`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html#onUpdate(io.deephaven.engine.table.TableUpdate)).

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

def letters = ["A", "B", "C", "D"]
def rng = new Random()

randomLetter = { ->
    return letters[rng.nextInt(4)]
}

sourceA = timeTable("PT2S").updateView("X = ii")
sourceB = timeTable("PT2S")
    .update("Letter = randomLetter()", "Y = randomDouble(0, 10)")
    .dropColumns("Timestamp")
    .lastBy("Letter")


listener = new InstrumentedTableUpdateListenerAdapter("listener", sourceA, false) {

    @Override
    boolean canExecute(long step) {
        // Wait until sourceB has finished processing the current update cycle
        return super.canExecute(step) && sourceB.satisfied(step)
    }

    @Override
    void onUpdate(TableUpdate upstream) {
        def x = sourceA.getColumnSource("X", long.class)
        upstream.added().forAllRowKeys { long rowKey ->
            println "From Source A: ${x.getLong(rowKey)}"
        }

        def iterator = sourceB.columnIterator("Y")
        def sourceBdata = ""

        try {
            while (iterator.hasNext()) {
                sourceBdata = sourceBdata + iterator.next() + ", "
            }
        } finally {
            iterator.close()
        }

        println "From Source B: ${sourceBdata}"
    }
}

sourceA.addUpdateListener(listener)
```

![Log readout from the listener](../assets/how-to/listener-dependency.gif)

> [!WARNING]
> Don't create derived tables inside the listener. A table you derive inside [`onUpdate`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html#onUpdate(io.deephaven.engine.table.TableUpdate)) is not one the listener waits on, so reading it may return inconsistent data. Derive tables before you add the listener, and include each derived table in the [`canExecute`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html#canExecute(long)) check, as the example above does for `sourceB`.

## Update graph locks and thread safety

When code running outside the Deephaven console creates a ticking table and adds a listener, the table may update between the moment you create it and the moment you register the listener. If this happens, the listener misses those updates.

The [Update Graph](../conceptual/table-update-model.md) coordinates all table updates in Deephaven. By holding a lock while creating the table _and_ adding the listener, you ensure no updates occur in between.

Deephaven provides two types of locks:

- **Shared lock** ([`sharedLock`](/core/javadoc/io/deephaven/engine/updategraph/UpdateGraph.html#sharedLock())): Pauses table updates while your code runs. Use this for table operations like creating tables and adding listeners.
- **Exclusive lock** ([`exclusiveLock`](/core/javadoc/io/deephaven/engine/updategraph/UpdateGraph.html#exclusiveLock())): Blocks all Update Graph activity. Use this only for advanced cases, such as waiting for specific update conditions.

![Diagram showing how locks prevent missed updates](../assets/how-to/update-graph-lock.png)

> [!NOTE]
> Code you run in the Deephaven console already runs under the Update Graph's exclusive lock, so no updates can slip between the lines of a single console execution. Updates can still happen between separate executions. This section applies to code running on other threads, such as timer callbacks or background workers.

> [!NOTE]
> Background threads don't have an execution context by default. Table operations like [`timeTable`](../reference/table-operations/create/timeTable.md) require a valid context. Capture the context before dispatching, then reopen it on the background thread. See [Execution context](../conceptual/execution-context.md) for details.

**Separate locks (race condition):**

The following examples run on a background thread that [`Thread.start`](https://docs.groovy-lang.org/latest/html/groovy-jdk/java/lang/Thread.html#start(java.lang.Thread,%20groovy.lang.Closure)) creates. Assigning `t` and `listener` without `def` stores them in the script's variables, so they stay reachable after the thread ends.

If you lock each operation separately, a race window exists between them:

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter
import io.deephaven.util.SafeCloseable

// Capture the execution context before starting the thread
ctx = ExecutionContext.getContext()

Thread.start {
    try (SafeCloseable ignored = ctx.open()) {
        // First lock: create the table
        ctx.getUpdateGraph().sharedLock().doLocked(() -> {
            t = timeTable("PT1S").update("X = i")
        })
        // RACE WINDOW: the table can update here, before the listener is added
        ctx.getUpdateGraph().sharedLock().doLocked(() -> {
            listener = new InstrumentedTableUpdateListenerAdapter("listener", t, false) {
                @Override
                void onUpdate(TableUpdate upstream) { println "update=${upstream}" }
            }
            t.addUpdateListener(listener)
        })
    }
}
```

**Single lock (safe):**

Wrap both operations in a single lock to eliminate the race:

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter
import io.deephaven.util.SafeCloseable

// Capture the execution context before starting the thread
ctx = ExecutionContext.getContext()

Thread.start {
    try (SafeCloseable ignored = ctx.open()) {
        // One lock: create the table and add the listener together
        ctx.getUpdateGraph().sharedLock().doLocked(() -> {
            t = timeTable("PT1S").update("X = i")
            listener = new InstrumentedTableUpdateListenerAdapter("listener", t, false) {
                @Override
                void onUpdate(TableUpdate upstream) { println "update=${upstream}" }
            }
            t.addUpdateListener(listener)
        })
    }
}
```

> [!NOTE]
> Keep locks brief. Tables cannot update while a lock is held, so long-held locks can delay data.

For more details on locks and thread safety, see [Synchronization and locking](../conceptual/query-engine/engine-locking.md).

## Reduce data volumes

Tables often tick at high frequencies and with large quantities of incoming data. Listen only to the data an operation needs. [Filters](./use-filters.md) and [reduced tick frequencies](./performance/reduce-update-frequency.md) cut both the amount of data a listener receives and how often it arrives.

The following example filters a table and reduces its tick frequency before listening to it.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

source = timeTable("PT0.5S").update("X = i").tail(5)
// Rename Timestamp so it doesn't clash with source's Timestamp column in the snapshot
trigger = timeTable("PT2S").renameColumns("DateTime = Timestamp")
result = source.where("X % 2 = 0").snapshotWhen(trigger)

h1 = new InstrumentedTableUpdateListenerAdapter(result, false) {
    @Override
    public void onUpdate(TableUpdate upstream) {
        println "LISTENER for even values: update=${upstream}"
    }
}

result.addUpdateListener(h1)
```

![`h1` receives filtered and downsampled updates](../assets/how-to/listener-downsampled.gif)

## Example: send and cancel orders

This example follows the order scenario from the start of this page. `orders` holds the five most recent orders, so each new order pushes the oldest one out. The listener reads the added rows to send new orders and the removed rows to cancel old ones. In a real application, it would call a broker's API instead of printing.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

orders = timeTable("PT1S").update("OrderId = ii").tail(5)

listener = new InstrumentedTableUpdateListenerAdapter("listener", orders, false) {
    @Override
    void onUpdate(TableUpdate upstream) {
        def orderId = orders.getColumnSource("OrderId", long.class)

        upstream.added().forAllRowKeys { long rowKey ->
            println "Send order ${orderId.getLong(rowKey)} to the broker"
        }
        // Removed rows have pre-shift keys, so read their previous values
        upstream.removed().forAllRowKeys { long rowKey ->
            println "Cancel order ${orderId.getPrevLong(rowKey)} with the broker"
        }
    }
}

orders.addUpdateListener(listener)
```

![`orders` ticks while the listener prints a send line for each new order and a cancel line for each order that leaves the table](../assets/how-to/listener-orders.gif)

## Related documentation

- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`TableUpdate`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html)
- [`Table`](/core/javadoc/io/deephaven/engine/table/Table.html)
- [`InstrumentedTableUpdateListenerAdapter`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html)
- [`MergedListener`](/core/javadoc/io/deephaven/engine/table/impl/MergedListener.html)
- [Synchronization and locking](../conceptual/query-engine/engine-locking.md)
- [`UpdateGraph`](/core/javadoc/io/deephaven/engine/updategraph/UpdateGraph.html)
