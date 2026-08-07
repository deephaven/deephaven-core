---
title: Listen to ticking tables
sidebar_label: Table listeners
---

Deephaven makes it easy to create dynamic queries that update in real time. When a table updates, a message describing the changes is sent to all listeners of the table. This mechanism is what makes ticking queries work. It can also be used to create new, dynamic functionality.

As an example, consider using a Deephaven query to create a dynamic table that monitors for situations needing human intervention. You can create a table listener that sends a Slack message every time one or more tables tick. Similarly, you could have a table of orders to buy or sell stocks. If rows are added to the order table, new orders are sent to the broker, and if rows are removed from the order table, orders are canceled with the broker.

This guide will show you how to create your own table listeners in Groovy.

## What is a table listener?

A table listener is an object that listens to one or more tables for updates. When connected to a ticking table, a listener receives one or more [`TableUpdate`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html) objects that can be used to access the added, modified, or removed data.

## Listen to one ticking table

To listen to a table, add an instance of the listener to the table with the [`Table.addUpdateListener(listener)`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#addUpdateListener(io.deephaven.engine.table.TableUpdateListener)) method. Once a listener is registered, it will begin receiving updates. To control what the listener does upon receiving an update, override the listener's [`onUpdate`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html#onUpdate(io.deephaven.engine.table.TableUpdate)) method.

In this simple example, the listener will keep track of how many times it has been called.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

t1 = timeTable("PT1S").update("X=i").tail(5)

h1 = new InstrumentedTableUpdateListenerAdapter(t1, false) {
    def counter = 0

    @Override
    public void onUpdate(TableUpdate upstream) {
        println "FUNCTION LISTENER: counter=${++counter} update=${upstream}"
    }
}

t1.addUpdateListener(h1)
```

![`t1` updates while `handle` prints updates to the log](../assets/how-to/listener-one-table.gif)

## Listen to multiple ticking tables

To listen to more than one table at once, you will need to use a [`MergedListener`](/core/javadoc/io/deephaven/engine/table/impl/MergedListener.html). Once a listener is registered, it will begin receiving updates.

A [`MergedListener`](/core/javadoc/io/deephaven/engine/table/impl/MergedListener.html) takes four inputs:

- `recorders`: an iterable set of [`ListenerRecorder`](/core/javadoc/io/deephaven/engine/table/impl/ListenerRecorder.html) instances.
- `dependencies`: An iterable set of dependencies (such as other Tables) for the [`MergedListener`](/core/javadoc/io/deephaven/engine/table/impl/MergedListener.html).
- `listenerDescription`: A String description of the listener.
- `result`: a result table that uses the listener's tables as sources. Can be null.

The following example listens to two time tables, one ticking every second and the other ticking every two seconds. [`getUpdate`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/ListenerRecorder.html#getUpdate()) returns a [`TableUpdate`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html) object for each [`ListenerRecorder`](/core/javadoc/io/deephaven/engine/table/impl/ListenerRecorder.html), and the [`MergedListener`](/core/javadoc/io/deephaven/engine/table/impl/MergedListener.html)'s `process` function is overwritten to print updates if they have been received and to do nothing otherwise.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.ColumnSource
import io.deephaven.engine.table.impl.ListenerRecorder
import io.deephaven.engine.table.impl.MergedListener
import io.deephaven.engine.rowset.RowSet
import java.time.Instant

t1 = timeTable("PT1S").update("X=i").tail(5)
t2 = timeTable("PT2S").update("X=i*2").tail(5)

recorder1 = new ListenerRecorder("t1", t1, null)
recorder2 = new ListenerRecorder("t2", t2, null)

h = new MergedListener([recorder1, recorder2], [], "Description", null) {
  def counter = 0

  @Override
  void process() {
    counter++
    TableUpdate tu1 = recorder1.getUpdate()
    TableUpdate tu2 = recorder2.getUpdate()

    ColumnSource<Instant> colSrc1 = t1.getColumnSource("Timestamp", Instant.class)
    ColumnSource<Integer> colSrc2 = t1.getColumnSource("X", int.class)
    ColumnSource<Instant> colSrc3 = t2.getColumnSource("Timestamp", Instant.class)
    ColumnSource<Integer> colSrc4 = t2.getColumnSource("X", int.class)

    if (tu1 != null) {
        RowSet.Iterator iter1 = tu1.added().iterator()
        while (iter1.hasNext()) {
            long rowKey = iter1.next()
            Instant col1Data = DateTimeUtils.epochNanosToInstant(colSrc1.getLong(rowKey))
            Class<?> col1Type = colSrc1.getType()
            int col2Data = colSrc2.getInt(rowKey)
            Class<?> col2Type = colSrc2.getType()
            println "t1 updates: {'Timestamp': [data=${col1Data}, ${col1Type}], 'X': [data=${col2Data}, ${col2Type}]"
        }
    }
    if (tu2 != null) {
        RowSet.Iterator iter2 = tu2.added().iterator()
        while (iter2.hasNext()) {
            long rowKey = iter2.next()
            Instant col3Data = DateTimeUtils.epochNanosToInstant(colSrc3.getLong(rowKey))
            Class<?> col3Type = colSrc3.getType()
            int col4Data = colSrc4.getInt(rowKey)
            Class<?> col4Type = colSrc4.getType()
            println "t2 updates: {'Timestamp': [data=${col3Data}, ${col3Type}], 'X': [data=${col4Data}, ${col4Type}]"
        }
    }
  }
}

recorder1.setMergedListener(h)
recorder2.setMergedListener(h)

t1.addUpdateListener(recorder1)
t2.addUpdateListener(recorder2)
```

![`t1` and `t2` update while `handle` prints updates to the log](../assets/how-to/listener-merged.gif)

> [!IMPORTANT]
> The `TableUpdate` returned by `getUpdate()` is `null` for any table that has not changed during the update cycle. These `null` values must be handled to avoid raising errors.

## Access table data

A [`TableUpdate`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html) object contains the added, modified, and removed rows from a table. There are several ways to access this data.

The following methods return a RowSet of the added, removed, or modified data:

- [`acquire`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/TableUpdate.html#acquire()) - Increments the reference count for the update.
- [`added`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/TableUpdate.html#added()) - rows added during the current update cycle.
- [`modified`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/TableUpdate.html#modified()) - rows modified during the current update cycle.
- [`removed`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/TableUpdate.html#removed()) - rows removed during the current update cycle.
- [`getModifiedPreShift`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/TableUpdate.html#getModifiedPreShift()) - modified rows in their pre-shift row key positions (before any shifts were applied during this update cycle).

The following example listens to added rows during each update cycle. It prints the data as the listener receives it.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.ColumnSource
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter
import io.deephaven.engine.rowset.RowSet
import java.time.Instant

t1 = timeTable("PT1S").update("X=100+i").tail(5)

h1 = new InstrumentedTableUpdateListenerAdapter(t1, false) {

    @Override
    public void onUpdate(TableUpdate upstream) {
        RowSet.Iterator iter = upstream.added().iterator()
        ColumnSource<Instant> colSrc1 = t1.getColumnSource("Timestamp", Instant.class)
        ColumnSource<Integer> colSrc2 = t1.getColumnSource("X", int.class)

        while (iter.hasNext()) {
            long rowKey = iter.next()
            Instant col1data = DateTimeUtils.epochNanosToInstant(colSrc1.getLong(rowKey))
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

t1 = timeTable("PT0.1s").update("X = i").lastBy()

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
            long prevRowIdx = prevIt.next()
            long currRowIdx = currIt.next()
            int prev = xCol.getPrevInt(prevRowIdx)
            int curr = xCol.getInt(currRowIdx)
            println "Change previous=${prev} current=${curr}"
        }
    }
}

t1.addUpdateListener(h1)
```

![`table` updates while `handle` prints modified rows](../assets/how-to/listener-mod-prev.gif)

## Add and remove listeners

Most applications that require the use of a table listener do so for the entirety of the application's lifetime. If a listener should only be registered for a specified period of time, a listener can be removed from a table using the [`Table.removeUpdateListener(listener)`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/TableAdapter.html#removeUpdateListener(io.deephaven.engine.table.TableUpdateListener)) method.

The following example uses [`java.util.Timer`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Timer.html) to remove a listener after 3 seconds and then adds it back it after 6 seconds.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter
import io.deephaven.engine.context.ExecutionContext;

class ExampleListener extends InstrumentedTableUpdateListenerAdapter {

    ExampleListener(String description, Table source, boolean retain) {
        super(description, source, retain)
    }

    @Override
    void onUpdate(TableUpdate upstream) {
        println "FUNCTION LISTENER: update=${upstream}"
    }
}

// Use an update graph lock to ensure that the listener does not miss the first updates to the table
ExecutionContext.getContext().getUpdateGraph().exclusiveLock().doLocked(() -> {
    t = timeTable("PT1s").update("X=i").tail(5)
    listener = new ExampleListener("Test Listener", t, true)
    println "Adding listener"
    t.addUpdateListener(listener)
});

new Timer().runAfter(3000) {
    println "Removing listener"
    t.removeUpdateListener(listener)
}

// Use an update graph lock to ensure that the listener does not miss updates during the 6-second wait
ExecutionContext.getContext().getUpdateGraph().exclusiveLock().doLocked(() -> {
    new Timer().runAfter(6000) {
        println "Adding listener"
        t.addUpdateListener(listener)
    }
});
```

![A listener is added and removed](../assets/how-to/listener-lock.gif)

## Error handling

If a listener encounters an error, it should throw an exception rather than catching and suppressing it. When a listener throws an exception:

- In a **Persistent Query**, the exception crashes the query — which is the correct behavior, as it alerts you that something is wrong.
- In a **Code Studio**, the error appears in the logs.

The following example throws a `RuntimeException` if it receives a value greater than 9. The exception propagates and causes the listener to fail, which is the expected behavior for a broken listener.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

source  = timeTableBuilder()
    .period("PT0.33S")
    .blinkTable(true)
    .build()
    .update("X = randomDouble(0,10)")
    .dropColumns("Timestamp")

listener = new InstrumentedTableUpdateListenerAdapter("listener", source, false) {

    @Override
    void onUpdate(TableUpdate upstream) {
        def added = upstream.added()

        if (added == null || added.isEmpty()) {
            return
        }
        if (added.any{element -> element > 9}) {
            throw new RuntimeException("Value exceeds 9")
        }
    }
}

source.addUpdateListener(listener)
```

![`source` updates while `listener` throws an exception when a value exceeds 9](../assets/how-to/listener-error.gif)

## Reduce data volumes

Tables often tick at high frequencies and with large quantities of incoming data. It's best practice to only listen to what's required for an operation. In such cases, applying [filters](./filters.md) and/or [reducing tick frequencies](./performance/reduce-update-frequency.md) will reduce both the quantity and frequency of incoming data to a listener.

The following example listens to a table that has been filtered and had its tick frequency reduced to reduce the rate at which the listener receives data.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

t1 = timeTable("PT1S").update("X=i").tail(5)

h1 = new InstrumentedTableUpdateListenerAdapter(t1, false) {
    def counter = 0

    @Override
    public void onUpdate(TableUpdate upstream) {
        println "FUNCTION LISTENER for even values: update= ${upstream}"
    }
}

source = timeTable("PT0.5s").update("X=i").tail(5)
trigger = timeTable("PT2s").renameColumns("DateTime = Timestamp")
result = source.where("X % 2 = 0").snapshotWhen(trigger)
result.addUpdateListener(h1)
```

![`handle` receives filtered and downsampled updates](../assets/how-to/listener-downsampled.gif)

## Replay data

A table listener can listen to data that existed before the listener was registered. For example, a listener that isn't registered until 10 seconds after a table starts ticking can be made to listen to the data that was created during those 10 seconds.

To make a listener listen to previously existing data, set the `replayInitialImage` parameter to `true` when calling [`addUpdateListener`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/TableAdapter.html#addUpdateListener(io.deephaven.engine.table.ShiftObliviousListener,boolean)).

The following example registers two listeners with a time table a few seconds after it's created. Only the one that sets `replayInitialImage` to `true` receives data when it's first registered.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.ShiftObliviousInstrumentedListenerAdapter

source = timeTable("PT0.3s").update("X = i")

listener = new ShiftObliviousInstrumentedListenerAdapter("LISTENER", source, false) {

    @Override
    void onUpdate(RowSet added, RowSet removed, RowSet modified) {
        println "FUNCTION LISTENER for even values: update={added=${added}, removed=${removed}, modified=${modified}}"
    }
}

//Wait a few seconds, and then run the next two code blocks independently

// Set replayInitialImage=false to begin receiving updates on the cycle that the listener is added
source.addUpdateListener(listener, false)
source.removeUpdateListener(listener)

// Set replayInitialImage=true to replay data that existed in the table before the listener was added
source.addUpdateListener(listener, true)
```

![`handle_replay` receives all the data the table started with, while `handle_no_replay` only receives the updates after it was registered](../assets/how-to/listener-replay.gif)

## Dependent tables

Listeners can use data from tables other than the one they are listening to if the additional tables are configured as dependencies. When one or more tables are listed as a dependency to a listener, the query engine will wait to call the listener until all dependent tables have been processed. When a table is not listed as a dependency, it may be in an inconsistent state when accessed.

> [!WARNING]
> Don't do table operations inside the listener. While performing operations on the dependent tables in the listener is safe, it is not recommended because reading or operating on the result tables of those operations may not be safe. It is best to perform the operations on the dependent tables beforehand and then add the result tables as dependencies to the listener so that they can be safely read in it.

For example, consider two tables, `sourceA` and `sourceB`, that tick simultaneously but cannot be joined. When listening to `sourceA`, it is not guaranteed that `sourceB` will have its updates processed in full before the listener receives the update from `sourceA`. To guarantee that all data is processed before the listener triggers, `sourceB` must be registered as a dependency for the listener.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.BaseTable.ListenerImpl

def letters = ["A", "B", "C", "D"]
def rng = new Random()

randomLetter = { ->
    return letters[rng.nextInt(4)]
}

sourceA = timeTable("PT2s").updateView("X = ii")
sourceB = timeTable("PT5s")
    .update("Letter = randomLetter()", "Y = randomDouble(0, 10)")
    .dropColumns("Timestamp")
    .lastBy("Letter")


listener = new ListenerImpl("listener", sourceA, sourceB) {

    @Override
    void onUpdate(TableUpdate upstream) {
        added = upstream.added()
        println "From Source A: ${added}"
        dependentData = getDependent().select("Y")
        iterator = dependentData.columnIterator("Y")
        sourceBdata = ""

        while (iterator.hasNext()) {
            sourceBdata = sourceBdata + iterator.next() + ", "
        }

        println "From Source B: ${sourceBdata}"

    }
}

sourceA.addUpdateListener(listener)
```

![Log readout from the listener](../assets/how-to/listener-dependency.gif)

## Example

Table listeners are often used to trigger actions based on table updates. For example, a listener could notify Slack or send an email when data meets some criteria. The following example prints values that meet certain criteria. In a real-world use case, rather than print an outlier value, a notification could be sent to relevant parties via email, Slack, Discord, or other service.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

source  = timeTableBuilder()
    .period("PT0.33S")
    .blinkTable(true)
    .build()
    .update("X = randomDouble(0,10)")
    .dropColumns("Timestamp")

listener = new InstrumentedTableUpdateListenerAdapter("listener", source, false) {

    @Override
    void onUpdate(TableUpdate upstream) {
        added = upstream.added()

        if (added == null) {
            return
        }
        if (added.any{element -> element > 9}) {
            println "value over 9 detected!"
        }
    }
}

source.addUpdateListener(listener)
```

![Log readout from the listener](../assets/how-to/listener-over9.gif)

## Related documentation

- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`TableUpdate`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html)
- [Table](/core/javadoc/io/deephaven/engine/table/Table.html)
