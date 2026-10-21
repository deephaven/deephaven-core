---
title: Triage errors in queries
---

Everyone makes mistakes when writing queries in the Deephaven Query Language, as in any programming language. Mistakes often result in queries failing to start up properly or crashing at some point during their runtime. This guide shows you how to:

- Read a stack trace to find the root cause.
- Recognize and fix common mistakes, including problems that produce wrong results or pauses instead of an exception.
- Check your [query strings](./query-string-overview.md) for errors.
- Get help, including what to put in a bug report, when you can't find the cause.

## Read a stack trace

At this point, you have written some Deephaven queries that _should_ produce some interesting tables and visualizations, but when you attempt to run the query, it fails.

The first step is to find the error (an exception) and isolate its root cause. When you run code in the IDE console, the error appears in the console output. The [Log panel](./logs.md#log-panel) shows the same error with its log header.

The log displays a large block of text that describes the exception and a chain of further exceptions that leads to the root of the problem.

> [!TIP]
> Identify the root cause by looking at the `Caused by` lines. Start from the last exception in the chain and work backwards.

For example, the following code creates a function that throws an error and a table that calls it, passing the row index [special variable](../reference/query-language/variables/special-variables.md) `i`:

```groovy should-fail
throwFunc = { val -> throw new RuntimeException(new IllegalStateException("Error: " + val)) }

x = emptyTable(1).update("X = throwFunc(i)")
```

<details className="error">
<summary>The query fails with the following error: </summary>

```
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: io.deephaven.engine.exceptions.TableInitializationException: Error while initializing Update([X]): an exception occurred while performing the initial select or update
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$32(QueryTable.java:1563)
	at io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder.withNugget(QueryPerformanceRecorder.java:369)
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$33(QueryTable.java:1500)
	at io.deephaven.engine.table.impl.QueryTable.memoizeResult(QueryTable.java:3639)
	at io.deephaven.engine.table.impl.QueryTable.selectOrUpdate(QueryTable.java:1499)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:1477)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:100)
	at io.deephaven.api.TableOperationsDefaults.update(TableOperationsDefaults.java:94)
	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
	at org.codehaus.groovy.runtime.callsite.PlainObjectMetaMethodSite.doInvoke(PlainObjectMetaMethodSite.java:48)
	at org.codehaus.groovy.runtime.callsite.PojoMetaMethodSite$PojoCachedMethodSite.invoke(PojoMetaMethodSite.java:186)
	at org.codehaus.groovy.runtime.callsite.PojoMetaMethodSite.call(PojoMetaMethodSite.java:51)
	at org.codehaus.groovy.runtime.callsite.CallSiteArray.defaultCall(CallSiteArray.java:47)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.call(AbstractCallSite.java:125)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.call(AbstractCallSite.java:139)
	at io.deephaven.dynamic.Script_8.run(Script_8.groovy:4)
	at groovy.lang.GroovyShell.evaluate(GroovyShell.java:427)
	at groovy.lang.GroovyShell.evaluate(GroovyShell.java:461)
	at groovy.lang.GroovyShell.evaluate(GroovyShell.java:436)
	at io.deephaven.engine.util.GroovyDeephavenSession.lambda$evaluate$0(GroovyDeephavenSession.java:352)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:51)
	at io.deephaven.engine.util.GroovyDeephavenSession.evaluate(GroovyDeephavenSession.java:352)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$0(AbstractScriptSession.java:163)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:196)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:207)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:195)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:163)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:72)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:75)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$4(ConsoleServiceGrpcImpl.java:191)
	at io.deephaven.server.session.SessionState$ExportBuilder.lambda$submit$2(SessionState.java:1537)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:995)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.server.runner.scheduler.SchedulerModule$ThreadFactory.lambda$newThread$0(SchedulerModule.java:100)
	at java.base/java.lang.Thread.run(Thread.java:1583)
Caused by: io.deephaven.engine.table.impl.select.FormulaEvaluationException: In formula: X = throwFunc(i)
	at io.deephaven.temp.c_610268fe304a124aa60dfecb6506ae2180bd289378e92df5a11d104fb0ea1713v65_0.Formula.applyFormulaPerItem(Formula.java:166)
	at io.deephaven.temp.c_610268fe304a124aa60dfecb6506ae2180bd289378e92df5a11d104fb0ea1713v65_0.Formula.lambda$fillChunkHelper$5(Formula.java:155)
	at io.deephaven.engine.rowset.RowSequence.lambda$forAllRowKeys$0(RowSequence.java:175)
	at io.deephaven.engine.rowset.impl.singlerange.SingleRangeMixin.forEachRowKey(SingleRangeMixin.java:17)
	at io.deephaven.engine.rowset.RowSequence.forAllRowKeys(RowSequence.java:174)
	at io.deephaven.temp.c_610268fe304a124aa60dfecb6506ae2180bd289378e92df5a11d104fb0ea1713v65_0.Formula.fillChunkHelper(Formula.java:152)
	at io.deephaven.temp.c_610268fe304a124aa60dfecb6506ae2180bd289378e92df5a11d104fb0ea1713v65_0.Formula.fillChunk(Formula.java:124)
	at io.deephaven.engine.table.impl.sources.ViewColumnSource.fillChunk(ViewColumnSource.java:219)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doApplyUpdate(SelectColumnLayer.java:413)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.lambda$doSerialApplyUpdate$2(SelectColumnLayer.java:265)
	at io.deephaven.engine.util.systemicmarking.SystemicObjectTracker.executeSystemically(SystemicObjectTracker.java:56)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doSerialApplyUpdate(SelectColumnLayer.java:264)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer$1.lambda$onAllRequiredColumnsCompleted$1(SelectColumnLayer.java:213)
	at io.deephaven.engine.table.impl.util.ImmediateJobScheduler.lambda$submit$0(ImmediateJobScheduler.java:40)
	at io.deephaven.engine.table.impl.util.ImmediateJobScheduler.submit(ImmediateJobScheduler.java:54)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer$1.onAllRequiredColumnsCompleted(SelectColumnLayer.java:211)
	at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer$SelectLayerCompletionHandler.onLayerCompleted(SelectAndViewAnalyzer.java:659)
	at io.deephaven.engine.table.impl.select.analyzers.BaseLayer.applyUpdate(BaseLayer.java:66)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.applyUpdate(SelectColumnLayer.java:152)
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$32(QueryTable.java:1550)
	... 38 more
Caused by: java.lang.RuntimeException: java.lang.IllegalStateException: Error: 0
	at java.base/jdk.internal.reflect.DirectConstructorHandleAccessor.newInstance(DirectConstructorHandleAccessor.java:62)
	at java.base/java.lang.reflect.Constructor.newInstanceWithCaller(Constructor.java:502)
	at java.base/java.lang.reflect.Constructor.newInstance(Constructor.java:486)
	at org.codehaus.groovy.reflection.CachedConstructor.invoke(CachedConstructor.java:72)
	at org.codehaus.groovy.runtime.callsite.ConstructorSite$ConstructorSiteNoUnwrapNoCoerce.callConstructor(ConstructorSite.java:105)
	at org.codehaus.groovy.runtime.callsite.CallSiteArray.defaultCallConstructor(CallSiteArray.java:59)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.callConstructor(AbstractCallSite.java:263)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.callConstructor(AbstractCallSite.java:277)
	at io.deephaven.dynamic.Script_8$_run_closure1.doCall(Script_8.groovy:2)
	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
	at org.codehaus.groovy.reflection.CachedMethod.invoke(CachedMethod.java:107)
	at groovy.lang.MetaMethod.doMethodInvoke(MetaMethod.java:323)
	at org.codehaus.groovy.runtime.metaclass.ClosureMetaClass.invokeMethod(ClosureMetaClass.java:274)
	at groovy.lang.MetaClassImpl.invokeMethod(MetaClassImpl.java:1030)
	at groovy.lang.Closure.call(Closure.java:427)
	at groovy.lang.Closure.call(Closure.java:416)
	at io.deephaven.temp.c_610268fe304a124aa60dfecb6506ae2180bd289378e92df5a11d104fb0ea1713v65_0.Formula.applyFormulaPerItem(Formula.java:164)
	... 57 more
Caused by: java.lang.IllegalStateException: Error: 0
	... 75 more
```

</details>

Deephaven stack traces tend to be very large, but they open with a summary of the failing operation. The block below is the trace above with the `at ...` frames removed. Its first line names the operation that failed. The remaining lines are the `Caused by:` lines:

```none {2-4}
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: io.deephaven.engine.exceptions.TableInitializationException: Error while initializing Update([X]): an exception occurred while performing the initial select or update
Caused by: io.deephaven.engine.table.impl.select.FormulaEvaluationException: In formula: X = throwFunc(i)
Caused by: java.lang.RuntimeException: java.lang.IllegalStateException: Error: 0
Caused by: java.lang.IllegalStateException: Error: 0
```

Following the tip above, read the chain from the last exception up:

- The last exception is the innermost error, an [`IllegalStateException`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/IllegalStateException.html) that our `throwFunc` function creates. The error occurs when the table tries to compute a value for the `X` column and invokes our function.
- Just above, [`RuntimeException`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/RuntimeException.html) appears as another `Caused by` line because `throwFunc` passes the [`IllegalStateException`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/IllegalStateException.html) to the [`RuntimeException`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/RuntimeException.html) constructor.
- Above that, the engine wraps the error in a [`FormulaEvaluationException`](/core/javadoc/io/deephaven/engine/table/impl/select/FormulaEvaluationException.html). The engine adds this wrapper whenever a [formula](./formulas.md) throws an exception, and its message names the formula that failed: `In formula: X = throwFunc(i)`.

This is a very simple example of triaging an equally simple query with a deliberate error. In practice, queries are much more sophisticated and contain many interdependencies between tables. The recipe for triage, however, is the same.

The Console in the Deephaven IDE is a critical tool during triage. After the stack trace isolates the failing operation, use it to execute parts of the query bit by bit, analyze the inputs to the failing method, and experiment with changes to solve the problem.

For a fuller walkthrough of Groovy error output, see [Decipher Groovy errors](./groovy-errors.md).

## Common mistakes

Once you've fixed the problems that stop a query from starting, it starts serving up tables and plots. The following sections show common mistakes, how to recognize them, and how to fix them.

Some queries fail as soon as they run. Others fail later, when new data brings an unexpected value. These runtime failures can be trickier to triage because they often depend on unexpected patterns in streaming data.

To triage either kind, follow the same steps as in [Read a stack trace](#read-a-stack-trace):

1. Find the exception.
2. List its `Caused by` lines.
3. Start at the last one and work backwards.

Two of the problems below, [Non-deterministic formulas](#non-deterministic-formulas) and [Garbage collection (GC) pauses](#garbage-collection-gc-pauses), show up as wrong results or pauses rather than an exception. Those sections describe what to look for instead.

### Unexpected null values

It's easy to forget that columns can contain [null values](./null-inf-nan.md). Calling a Java method on a null value, such as `Items.charAt(0)` when `Items` is null, fails with a [`NullPointerException`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/NullPointerException.html).

The following query creates a table with a [string column](../reference/table-operations/create/stringCol.md) that contains a null value. It then uses the `charAt` method to keep strings that start with `B`.

```groovy should-fail
table = newTable(stringCol("Items", "A", "B", null, "D"))

result = table.where("Items.charAt(0) == 'B'")
```

In this example, the last `Caused by` line of the stack trace contains the cause:

```none
Caused by: java.lang.NullPointerException: Cannot invoke "String.charAt(int)" because "<local7>" is null
```

`<local7>` is the JVM's name for a variable in the [filter](./filters.md) code that Deephaven generates. The number can differ.

To fix the [filter](./filters.md), check whether the value is null before you call a method on it:

```groovy order=table,result
table = newTable(stringCol("Items", "A", "B", null, "D"))

result = table.where("!isNull(Items) && Items.charAt(0) == 'B'")
```

> [!NOTE]
> Adding [`!isNull(Items)`](/core/javadoc/io/deephaven/function/Basic.html#isNull(T)) to the start of the [filter](./filters.md) works because of short-circuit evaluation. When [`!isNull(Items)`](/core/javadoc/io/deephaven/function/Basic.html#isNull(T)) is `false`, the engine skips the rest of the [`where`](../reference/table-operations/filter/where.md) clause, so `Items.charAt(0) == 'B'` never runs on a null value.

### String and array access

Reading past the end of a [string](../reference/query-language/types/strings.md), a [vector](../conceptual/arrays-vs-vectors.md), or a Java [array](../reference/query-language/types/arrays.md) behaves differently for each type:

- [Strings](../reference/query-language/types/strings.md): reading a character at a position that does not exist throws an exception.
- [Vectors](../conceptual/arrays-vs-vectors.md): reading an element past the end of a vector cell, such as one that [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) produces, returns null. The failure shows up later as an [unexpected null value](#unexpected-null-values) rather than as an exception.
- Java [arrays](../reference/query-language/types/arrays.md): reading an element past the end of a Java array cell, such as one holding `int[]` values, throws an [`ArrayIndexOutOfBoundsException`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/ArrayIndexOutOfBoundsException.html).

The following query creates a table with a [string column](../reference/table-operations/create/stringCol.md) that holds [strings](../reference/query-language/types/strings.md) of varying length. It then keeps the [strings](../reference/query-language/types/strings.md) whose second character is `i`.

```groovy should-fail
table = newTable(stringCol("Items", "Ai", "B", "Ci", "Da"))

result = table.where("Items.charAt(1) == 'i'")
```

As with the null-value error in [Unexpected null values](#unexpected-null-values), the last `Caused by` line contains the cause:

```none
Caused by: java.lang.StringIndexOutOfBoundsException: Index 1 out of bounds for length 1
```

As in [Read a stack trace](#read-a-stack-trace), the first line of the stack trace names the [`TableInitializationException`](/core/javadoc/io/deephaven/engine/exceptions/TableInitializationException.html). It also shows the [query string](./query-string-overview.md) that failed, `where([Items.charAt(1) == 'i'])`. The [filter](./filters.md) tried to read index 1 of a string that has only one character.

We can inspect the table by adding a new column `ItemsLength = Items.length()`:

```groovy order=length
table = newTable(stringCol("Items", "Ai", "B", "Ci", "Da"))

length = table.update("ItemsLength = Items.length()")
```

To see the rows where the string is less than two characters long, right-click the `ItemsLength` column header, open the [Advanced Filters](./user-interface/filters.md#advanced-filters) panel, and filter to values less than 2:

![The Advanced Filters panel, set to filter ItemsLength to less than 2](../assets/how-to/errors/triage2.png)

Guard the `charAt` call with a length check, the same way [Unexpected null values](#unexpected-null-values) guards it with a null check:

```groovy order=result
table = newTable(stringCol("Items", "Ai", "B", "Ci", "Da"))

result = table.where("Items.length() >= 2 && Items.charAt(1) == 'i'")
```

This works because of short-circuit evaluation. When `Items.length() >= 2` is `false`, the engine skips `Items.charAt(1)`, so it never reads past the end of a short string.

If you only care whether the string contains the `i` character, a better solution is:

```groovy order=result
table = newTable(stringCol("Items", "Ai", "B", "Ci", "Da"))
result = table.where("Items.contains(`i`)")
```

As always, use the Deephaven IDE to validate that this change works and produces the result you expect!

Vectors behave differently. The following query uses [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) to collect the `Value` column into a [vector](../conceptual/arrays-vs-vectors.md) for each `Key`, then reads the third element of each vector:

```groovy order=result
grouped = emptyTable(5).update("Key = ii % 2", "Value = (int) ii").groupBy("Key")
result = grouped.update("ThirdValue = Value[2]")
```

The vector for `Key` 1 has only two elements, so `ThirdValue` is null in that row. The query doesn't fail. When a query reads vector elements by position, look downstream for [unexpected null values](#unexpected-null-values).

### Join key problems

Deephaven's [join operations](./joins-exact-relational.md) are one of its most powerful features. They combine tables by matching values in key columns to produce new tables.

Some [joins](./joins-exact-relational.md) allow any number of matching rows on each side, but others do not. By default, [`naturalJoin`](../reference/table-operations/join/natural-join.md) allows at most one right-table row for each key that a left row matches. If a left row's key matches more than one right-table row, the [join](../reference/table-operations/join/natural-join.md) fails. The following query shows this:

```groovy should-fail
left = newTable(
    stringCol("LastName", "Rafferty"),
    intCol("DeptID", 31),
    stringCol("Telephone", "(303) 555-0162")
)
right = newTable(
    intCol("DeptID", 31, 33, 34, 31),
    stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
    stringCol("DeptTelephone", "(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171")
)

result = left.naturalJoin(right, "DeptID")
```

Because `31` appears twice in the `DeptID` column in the right table, the query fails. In this case, the first line of the stack trace already names the root cause:

```none
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: io.deephaven.engine.exceptions.DuplicateRightKeyException: Natural Join found duplicate right key for 31
```

If the right table can contain duplicate keys, pass `NaturalJoinType.FIRST_MATCH` or `NaturalJoinType.LAST_MATCH` as the `joinType` argument of [`naturalJoin`](../reference/table-operations/join/natural-join.md) to choose which match to keep. The Groovy console imports `io.deephaven.api.*` by default, so [`NaturalJoinType`](/core/javadoc/io/deephaven/api/NaturalJoinType.html) needs no import. The following query keeps the last match:

```groovy order=result
left = newTable(
    stringCol("LastName", "Rafferty"),
    intCol("DeptID", 31),
    stringCol("Telephone", "(303) 555-0162")
)
right = newTable(
    intCol("DeptID", 31, 33, 34, 31),
    stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
    stringCol("DeptTelephone", "(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171")
)

result = left.naturalJoin(right, "DeptID", NaturalJoinType.LAST_MATCH)
```

The [join](../reference/table-operations/join/natural-join.md) now succeeds. `LAST_MATCH` keeps the last right-table row for key `31`, so `DeptName` is `Marketing`.

### Non-deterministic formulas

Not all errors have stack traces. Some queries run without an error but produce wrong results.

A deterministic function is one that produces the _exact_ same result every single time when given the same input parameters. A non-deterministic function, such as a random number generator, does not. In a [formula column](./use-select-view-update.md), a non-deterministic function can return a different value each time the column is read. Consider the following query:

```groovy order=result,source
source = emptyTable(10).updateView("Number = randomInt(1, 10)")

result = source.update("NumberTimesTwo = Number * 2")
```

The `NumberTimesTwo` values are all even, but most are not twice the value in `Number`. Here is why:

- [`updateView`](../reference/table-operations/select/update-view.md) creates a [formula column](./use-select-view-update.md). A formula column stores the formula, not the values, and calculates each value on demand.
- Each time something reads `Number`, [`randomInt`](/core/javadoc/io/deephaven/function/Random.html#randomInt(int,int)) runs again and returns a new value.
- `NumberTimesTwo` reads `Number` again, so it doubles a different random number than the one `Number` shows.

When a [formula](./formulas.md) calls a non-deterministic function, create its column with [`update`](../reference/table-operations/select/update.md) instead of [`updateView`](../reference/table-operations/select/update-view.md), so that Deephaven calculates and stores each value once:

```groovy order=result,source
source = emptyTable(10).update("Number = randomInt(1, 10)")

result = source.update("NumberTimesTwo = Number * 2")
```

Now every `NumberTimesTwo` value is exactly twice `Number`.

Errors like these are trickier to diagnose than an exception, so pay attention to which kind of column you create when a formula calls a non-deterministic function.

### Out of memory

A common problem is running out of heap memory as data volume grows during the day. It happens when your Deephaven instance has too little heap. Unexpected data patterns, such as unusually large [Kafka](./data-import-export/kafka-stream.md) input, make it worse.

The most common out-of-memory error looks like this:

```none
Caused by: java.lang.OutOfMemoryError: Java heap space
```

Start by increasing the heap. If that isn't enough, review the query for changes that reduce heap usage. In practice, the fix often combines several of the techniques below.

To increase the heap allocated to the Java Virtual Machine (JVM), set `-Xmx` in the `START_OPTS` environment variable, which Docker and the `start` script read (for example, `START_OPTS=-Xmx8g`). If you run Deephaven with Docker, as in the [Docker install guide](../getting-started/docker-install.md), make sure Docker itself has enough memory for that heap. See [Configure and adjust heap size](./heap-size.md).

Increasing the heap is enough for many workloads. Be careful when you cross 32GB:

- Below about 32GB, the JVM uses compressed object pointers, which make each object reference smaller.
- At about 32GB and above, the JVM turns compressed object pointers off so it can address the whole heap.
- As a result, raising the heap from 30GB to 32GB leaves less usable heap for your query.

> [!TIP]
> If you must cross the 32GB boundary, increase the heap well past it, because uncompressed object pointers use more of the heap.

The next step is to review your query code.

- Where you use [`update`](../reference/table-operations/select/update.md), consider [`updateView`](../reference/table-operations/select/update-view.md). It stores the formula instead of the values, which trades CPU time for memory. For simple computations, this is often a good trade. See [Choose the right column selection method](./use-select-view-update.md#choose-the-right-column-selection-method).
  - Only switch deterministic [formulas](./formulas.md). [`updateView`](../reference/table-operations/select/update-view.md) recalculates values each time they are read, so a formula that uses random numbers returns different values each time. See [Non-deterministic formulas](#non-deterministic-formulas).
- Reuse tables instead of recomputing them. Look for duplicate tables, or tables that differ only in which columns they keep, and derive later tables from one shared table. Deephaven reuses the result of many repeated operations on the same table, but derive from a shared table explicitly rather than rely on that.
- Watch the number of unique keys in tables you pass to key-based operations such as [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md), [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md), and [`join`](../reference/table-operations/join/join.md). If keys are mostly unique within a table, such as a `SaleID` column, [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md) can produce millions of single-row tables, each with some heap overhead. In that case, consider different keys or different operations.
- Consider the order of your query operations. [Filter](../reference/table-operations/filter/where.md) data before you [join](./joins-exact-relational.md) tables.
  - A very simple example is applying a [`join`](../reference/table-operations/join/join.md) and a [`where`](../reference/table-operations/filter/where.md) condition. In ``derived = myTable.join(myOtherTable, "JoinColumn").where("FilterColumn = `FilterValue`")``, the [`join`](../reference/table-operations/join/join.md) matches rows from `myTable` that the [`where`](../reference/table-operations/filter/where.md) expression later filters out, so it uses much more heap than it needs to.
  - [Filter](../reference/table-operations/filter/where.md) first instead: ``derived = myTable.where("FilterColumn = `FilterValue`").join(myOtherTable, "JoinColumn")``. This uses less heap and reduces how many rows downstream tables process each time the left or right table updates.

A query can use more heap than expected for many other reasons. Finding them requires a deep analysis of how the query performs at runtime. You may need to use a Java profiling tool (such as [JProfiler](https://www.ej-technologies.com/products/jprofiler/overview.html)) to identify poorly performing sections of your code.

### Garbage collection (GC) pauses

GC pauses are another heap-related problem. They happen when your query has enough memory but processes a lot of data on each tick. A tick is one update cycle of a [ticking table](../conceptual/table-types.md), which can add, modify, or remove rows.

- A long series of query operations allocates a lot of temporary heap on each tick.
- When those computations finish, the JVM garbage collector reclaims that memory.
- Garbage collection uses CPU time. Long collections pause query processing, and computations back up.

The symptom is periodic long pauses while the JVM frees memory. When you see this, [reduce how often your tables tick](./performance/reduce-update-frequency.md).

## Checklist for query string errors

Before you file a bug, check your [query strings](./query-string-overview.md) against this list.

1. Is the [query string](./query-string-overview.md) itself properly quoted as a Groovy string?

   - [Query strings](./query-string-overview.md) in operations such as [`where`](../reference/table-operations/filter/where.md) or [`update`](../reference/table-operations/select/update.md) must be Groovy strings, in double or single quotes. For example, ``myTable.where("StringColumn = `TheValue`")``.
   - If you pasted an expression from another app, such as Slack, check for Unicode curly quotation marks (U+201C and U+201D). Some apps substitute them for the ASCII quotation mark (U+0022).

2. Do you have matching parentheses?

   - Make sure every opening parenthesis has a matching closing parenthesis.

3. If you are referring to a column in a Deephaven query expression:

   - Is the column name spelled and capitalized correctly?
     - A [formula](./formulas.md) or a [range filter](./filters.md#range-filters), such as `Price > 5`, that references a column that doesn't exist fails with a [`FormulaCompilationException`](/core/javadoc/io/deephaven/engine/table/impl/select/FormulaCompilationException.html), because the engine cannot compile it.
     - A simple [match filter](./filters.md#match-filters), such as `` StringColumn = `TheValue` ``, fails instead with a message that the column doesn't exist in this table, followed by the table's available columns.
   - Is the type of the column what you expect it to be?
   - Did you account for [null values](./null-inf-nan.md)?

4. If your query string contains [string literals](../reference/query-language/types/strings.md), did you quote them properly?

   - Inside a query string, quote string literals with backticks (`` ` ``) or Java double quotes (`"`), **not** single quotes (`'`), which Deephaven uses for character and [date-time literals](./date-time-literals.md).
   - Do you have matching close quotes for your open quotes?

5. If you are using [date-times](./date-time-literals.md) in your expressions:

   - Did you use single quotes (`'`), not double quotes (`"`) or backticks (`` ` ``)?
   - Did you use the proper format? The most common date-time format is `yyyy-MM-ddTHH:mm:ss[.SSSSSSSSS] TZ`. The fractional seconds are optional and can have up to nine digits. For example, `'2024-01-01T09:30:00 ET'` or `'2024-01-01T09:30:00.123 ET'`.

6. Are all classes that you are trying to use properly imported?

   - A [query string](./query-string-overview.md) can use a class by its simple name only if the query library imports it. The query library imports a set of [defaults](https://github.com/deephaven/deephaven-core/blob/main/engine/table/src/main/java/io/deephaven/engine/table/lang/impl/QueryLibraryImportsDefaults.java), and server configuration and installed extensions can add more.
   - A Groovy `import` statement makes a class available to your script, not to query strings.
   - To use any other Java class, write its fully-qualified name, such as ``java.net.URI.create(`https://deephaven.io`)``. Or import it with [`importClass`](/core/javadoc/io/deephaven/engine/context/QueryLibrary.html#importClass(java.lang.Class)) or [`importPackage`](/core/javadoc/io/deephaven/engine/context/QueryLibrary.html#importPackage(java.lang.Package)), and then use its simple name. For example, after `ExecutionContext.getContext().getQueryLibrary().importClass(java.net.URI.class)`, a query string can call ``URI.create(`https://deephaven.io`)``. See [Java objects and classes in query strings](./java-classes.md).

## Get help

If you've gone through all of the steps to identify the root of a problem, but can't find a cause in your query, you may have uncovered a bug. In this case, you should file a [bug report](https://github.com/deephaven/deephaven-core/issues). Be sure to include the following with your bug report:

- The Deephaven server and web UI versions. See [Get version information](./get-version.md).
- The complete stack trace, not just the list of `Caused by` lines.
- A code snippet or a description of the query.
- Any logs from your user console, or from your terminal if you run Deephaven in Docker.
- Support logs exported from the browser. Open **User Settings** and click **Export Logs** to download a zip with the browser logs and the current state of the application. See [Export support logs](./logs.md#export-support-logs).

You can also get help by asking questions in our [GitHub Discussions](https://github.com/deephaven/deephaven-core/discussions/categories/q-a) forum.

## Related documentation

- [Deephaven's table types](../conceptual/table-types.md)
- [Arrays vs vectors](../conceptual/arrays-vs-vectors.md)
- [Create a new table](./new-and-empty-table.md#newtable)
- [How to handle null, infinity, and not-a-number values](./null-inf-nan.md)
- [Joins: Exact and Relational](./joins-exact-relational.md)
- [How to select, view, and update data in tables](./use-select-view-update.md)
- [How to work with strings](./work-with-strings.md)
- [Filters in query strings](./filters.md)
- [Formulas in query strings](./formulas.md)
- [Java objects and classes in query strings](./java-classes.md)
- [Date-time literals in query strings](./date-time-literals.md)
- [Configure and adjust heap size](./heap-size.md)
- [Connect to a Kafka stream](./data-import-export/kafka-stream.md)
- [Retrieve logs](./logs.md)
- [Decipher Groovy errors](./groovy-errors.md)
- [Get version information](./get-version.md)
- [Query string overview](./query-string-overview.md)
- [Table filters in the UI](./user-interface/filters.md)
- [How to reduce the update frequency of ticking tables](./performance/reduce-update-frequency.md)
- [Install and run with Docker](../getting-started/docker-install.md)
- [Special variables](../reference/query-language/variables/special-variables.md)
- [Strings](../reference/query-language/types/strings.md)
- [Arrays](../reference/query-language/types/arrays.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`intCol`](../reference/table-operations/create/intCol.md)
- [`join`](../reference/table-operations/join/join.md)
- [`naturalJoin`](../reference/table-operations/join/natural-join.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
- [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`stringCol`](../reference/table-operations/create/stringCol.md)
- [`update`](../reference/table-operations/select/update.md)
- [`updateView`](../reference/table-operations/select/update-view.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`NaturalJoinType` Javadoc](/core/javadoc/io/deephaven/api/NaturalJoinType.html)
- [`QueryLibrary` Javadoc](/core/javadoc/io/deephaven/engine/context/QueryLibrary.html)
- [`Basic` Javadoc](/core/javadoc/io/deephaven/function/Basic.html)
- [`Random` Javadoc](/core/javadoc/io/deephaven/function/Random.html)
- [`FormulaCompilationException` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/select/FormulaCompilationException.html)
- [`FormulaEvaluationException` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/select/FormulaEvaluationException.html)
- [`TableInitializationException` Javadoc](/core/javadoc/io/deephaven/engine/exceptions/TableInitializationException.html)
