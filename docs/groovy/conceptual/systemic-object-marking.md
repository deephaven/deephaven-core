---
title: Systemic Object Marking
---

In Deephaven, a **systemic object** is an object whose failure the engine reports to the server, in addition to logging it. The engine logs every failure, but it reports only failures of systemic objects.

What happens after a systemic failure depends on the server:

- Deephaven Community Core writes an extra `Error in table update` entry to the server log and keeps running.
- Deephaven Core+ (Enterprise) terminates the worker.

Reporting is separate from failure propagation. When a table fails, the tables that depend on it also fail, whether or not they are systemic.

Systemic object marking lets you decide which objects are systemic:

- When marking is disabled (the default), the engine treats every table and [single-table listener](../how-to-guides/table-listeners-groovy.md) as systemic. A [`MergedListener`](/core/javadoc/io/deephaven/engine/table/impl/MergedListener.html) without a result table is the [exception](#manage-thread-systemic-status).
- When marking is enabled, a new table, or a [table listener](../how-to-guides/table-listeners-groovy.md) that listens to a single table, takes its status from the thread that creates it. It is systemic if that thread is marked systemic, and non-systemic otherwise.
- Deephaven marks its own [update graph](./dag.md) threads systemic. Other threads, including the one that runs your console code, are non-systemic unless you mark them.

When the engine creates tables later for an operation you ran, such as new constituents of a [partitioned table](../how-to-guides/partitioned-tables.md), they take the status of the thread that ran the operation, not that of the [update graph](./dag.md) thread.

Systemic object marking is most useful on Core+, where it lets a worker keep running when a non-critical table fails, such as one created while exploring data in the console. On Community Core, it decides only whether a failure also produces that `Error in table update` log entry.

Systemic objects are unrelated to the systemic [execution context](./execution-context.md) that a script session owns.

This guide first shows what happens when a table fails. It then explains how to enable marking, how to control which objects are systemic with the [`SystemicObjectTracker`](/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObjectTracker.html) class or by marking an existing table or listener, and how to set the systemic status of a script you run from the [Java client](../how-to-guides/java-client.md).

## What happens when a table fails

The difference between systemic and non-systemic objects is most visible when a [ticking table](./table-types.md) fails. In this example, the [formula](../how-to-guides/formulas.md) `Z = Y.toString()` throws a [`NullPointerException`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/NullPointerException.html) once `X` exceeds 10 and `Y` becomes null:

```groovy should-fail
timeBomb = timeTable("PT1s").update("X = ii", "Y = X > 10 ? null : `abc`", "Z = Y.toString()")
```

With marking enabled, console code runs on a non-systemic thread, so `timeBomb` is non-systemic unless you [mark it systemic](#set-systemic-status-for-a-block-of-code). With marking disabled, `timeBomb` is systemic.

After about 11 seconds, the table fails. Whether or not `timeBomb` is systemic, it and the tables that depend on it fail, and the engine logs the error. The difference is whether the engine also reports the error:

- If `timeBomb` is systemic, the engine reports the error. A Community Core server writes the `Error in table update` log entry and keeps running, and a Core+ worker terminates.
- If `timeBomb` is non-systemic, the engine doesn't report its failure. A Community Core server writes no `Error in table update` log entry, and a Core+ worker keeps running.

A non-systemic table's failure can still be reported through an object that depends on it. If a systemic table or [listener](../how-to-guides/table-listeners-groovy.md) depends on `timeBomb`, that object also fails, the engine reports its failure, and a Core+ worker terminates.

## Enable systemic object marking

Systemic object marking is disabled by default, and you can't turn it on or off while the server is running. To enable it, set the `SystemicObjectTracker.enabled` property to `true` when you start the server, for example with the JVM option:

```shell
-DSystemicObjectTracker.enabled=true
```

See [how to set configuration properties](../how-to-guides/configuration/configuration-properties.md#how-to-set-properties) for the ways to pass a JVM option or set a property in a [configuration file](../how-to-guides/configuration/config-file.md).

Check whether systemic object marking is enabled with [`isSystemicObjectMarkingEnabled`](/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObjectTracker.html#isSystemicObjectMarkingEnabled()):

```groovy test-set=1 order=:log
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker

println SystemicObjectTracker.isSystemicObjectMarkingEnabled()
```

## Manage thread systemic status

Because systemic status comes from the creating thread, you control which tables and single-table [listeners](../how-to-guides/table-listeners-groovy.md) are systemic by setting the status of the thread that creates them.

A [`MergedListener`](/core/javadoc/io/deephaven/engine/table/impl/MergedListener.html) is systemic only when its result table is. One constructed with a `null` result table is never systemic, whether or not marking is enabled, so the engine never reports its failures. The exception is a subclass that overrides [`systemicResult`](/core/javadoc/io/deephaven/engine/table/impl/MergedListener.html#systemicResult()).

Use [`isSystemicThread`](/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObjectTracker.html#isSystemicThread()) to check the current thread's status, and [`markThreadSystemic`](/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObjectTracker.html#markThreadSystemic()) or [`markThreadNotSystemic`](/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObjectTracker.html#markThreadNotSystemic()) to change it. If systemic object marking is not enabled, [`isSystemicThread`](/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObjectTracker.html#isSystemicThread()) always returns `false`, even though every table is then systemic, and [`markThreadSystemic`](/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObjectTracker.html#markThreadSystemic()) and [`markThreadNotSystemic`](/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObjectTracker.html#markThreadNotSystemic()) do nothing.

This example checks [`isSystemicObjectMarkingEnabled`](/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObjectTracker.html#isSystemicObjectMarkingEnabled()) first. When marking is enabled, it marks the current thread systemic and prints the thread's new status. Otherwise, it prints a message:

```groovy test-set=1 order=:log
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker

if (SystemicObjectTracker.isSystemicObjectMarkingEnabled()) {
    SystemicObjectTracker.markThreadSystemic()
    println SystemicObjectTracker.isSystemicThread()
} else {
    println "Systemic object marking is not currently enabled."
}
```

The new status lasts until you change it again, so console code you run later on the same thread also uses it. The server runs console code from every session on the same thread, so the new status also applies to code that other sessions and clients run later.

To change the status only for a few lines, use [`executeSystemically`](#set-systemic-status-for-a-block-of-code) instead.

## Set systemic status for a block of code

The [`executeSystemically`](/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObjectTracker.html#executeSystemically(boolean,java.util.function.Supplier)) method runs a closure with the current thread's systemic status set to its first argument, then restores the thread's previous status:

- `true` makes the objects the closure creates systemic.
- `false` makes them non-systemic.

If systemic object marking is not enabled, [`executeSystemically`](/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObjectTracker.html#executeSystemically(boolean,java.util.function.Supplier)) runs the closure without changing anything.

```groovy order=systemicT,notSystemicT
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker

// Systemic whether or not marking is enabled
systemicT = SystemicObjectTracker.executeSystemically(true, {
    emptyTable(1).update("Label = `I am systemic`")
})

// Non-systemic when marking is enabled (systemic otherwise)
notSystemicT = SystemicObjectTracker.executeSystemically(false, {
    emptyTable(1).update("Label = `I am not systemic`")
})
```

## Mark an existing table or listener systemic

To make an existing table systemic, call its [`markSystemic`](/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObject.html#markSystemic()) method. It returns a new, systemic table that shares the original's data. The original table stays as it was.

In this example, `systemicT` is systemic, and `t` keeps the status it had when it was created:

```groovy order=systemicT,t
t = emptyTable(1).update("Label = `I am not systemic`")
systemicT = t.markSystemic()
```

Tables you create from `systemicT` don't inherit its mark. They take their status from the thread that creates them.

On a single-table [listener](../how-to-guides/table-listeners-groovy.md), such as an [`InstrumentedTableUpdateListenerAdapter`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html), [`markSystemic`](/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObject.html#markSystemic()) marks the listener itself and returns it.

## Systemic scripts from the Java client

To set the systemic status of a script you run from the [Java client](../how-to-guides/java-client.md), pass [`ExecuteCodeOptions`](/core/javadoc/io/deephaven/client/impl/ExecuteCodeOptions.html) to [`ConsoleSession.executeCode`](/core/javadoc/io/deephaven/client/impl/ConsoleSession.html#executeCode(java.lang.String,io.deephaven.client.impl.ExecuteCodeOptions)). Build the options with the builder's [`executeSystemic`](/core/javadoc/io/deephaven/client/impl/ExecuteCodeOptions.Builder.html#executeSystemic(io.deephaven.client.impl.ExecuteCodeOptions.SystemicType)) method, which takes a [`SystemicType`](/core/javadoc/io/deephaven/client/impl/ExecuteCodeOptions.SystemicType.html):

- `SystemicType.Systemic` makes the objects the script creates systemic.
- `SystemicType.NotSystemic` makes them non-systemic.
- `SystemicType.ServerDefault`, the default, runs the script without changing the thread's status. When marking is enabled, its objects are non-systemic unless earlier console code marked that thread systemic.

When marking is disabled, the server ignores the setting, and every table and [single-table listener](../how-to-guides/table-listeners-groovy.md) the script creates is systemic.

In this example, `console` is a [`ConsoleSession`](/core/javadoc/io/deephaven/client/impl/ConsoleSession.html) and `criticalScript` is the script to run:

```java skip-test
import io.deephaven.client.impl.ExecuteCodeOptions;
import io.deephaven.client.impl.ExecuteCodeOptions.SystemicType;

ExecuteCodeOptions options = ExecuteCodeOptions.builder()
        .executeSystemic(SystemicType.Systemic)
        .build();
console.executeCode(criticalScript, options);
```

## Related documentation

- [Live DAG](./dag.md)
- [Table types](./table-types.md)
- [Execution Context](./execution-context.md)
- [Table listeners](../how-to-guides/table-listeners-groovy.md)
- [Partitioned tables](../how-to-guides/partitioned-tables.md)
- [Formulas](../how-to-guides/formulas.md)
- [Java client](../how-to-guides/java-client.md)
- [Configuration properties](../how-to-guides/configuration/configuration-properties.md)
- [Configuration file](../how-to-guides/configuration/config-file.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`SystemicObjectTracker` Javadoc](/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObjectTracker.html)
- [`SystemicObject` Javadoc](/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObject.html)
- [`MergedListener` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/MergedListener.html)
- [`InstrumentedTableUpdateListenerAdapter` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html)
- [`ExecuteCodeOptions` Javadoc](/core/javadoc/io/deephaven/client/impl/ExecuteCodeOptions.html)
- [`ExecuteCodeOptions.SystemicType` Javadoc](/core/javadoc/io/deephaven/client/impl/ExecuteCodeOptions.SystemicType.html)
- [`ConsoleSession` Javadoc](/core/javadoc/io/deephaven/client/impl/ConsoleSession.html)
