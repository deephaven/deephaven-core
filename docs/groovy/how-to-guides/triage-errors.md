---
title: Triage errors in queries
---

This guide will show you how to interpret query errors and exception messages.

As with any programming language, when writing queries in Deephaven Query Language, you will certainly make mistakes. Mistakes will often result in queries failing to start up properly or crashing at some point during their runtime. Here, we provide a set of best practices to use when interpreting and fixing these issues.

## Simple triage

At this point, you have written some Deephaven queries that _should_ produce some interesting tables and visualizations, but when you attempt to run the query, it fails.

The first step is to find the error (referred to as an Exception) and isolate its root cause. This will usually appear in the log panel.

The log will display a large block of text that describes the exception and a chain of further exceptions that leads to the root of the problem.

> [!TIP]
> Identify the root cause by looking at the `caused by` statements. Start from the last exception in the chain and work backwards.

For example, we'll start by creating a function that generates an error, then a table that invokes it, with the following commands:

```groovy should-fail
throwFunc = { val -> throw new RuntimeException(new IllegalStateException("Error: " + val))}

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

Deephaven stack traces tend to be very large, but lead off with a summation of the underlying error. Look at the first few lines of the above stack trace:

```none {2,4}
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: io.deephaven.engine.exceptions.TableInitializationException: Error while initializing Update([X]): an exception occurred while performing the initial select or update
Caused by: io.deephaven.engine.table.impl.select.FormulaEvaluationException: In formula: X = throwFunc(i)
Caused by: java.lang.RuntimeException: java.lang.IllegalStateException: Error: 0
Caused by: java.lang.IllegalStateException: Error: 0
```

When you interpret these exception chains, it's best to start from the last exception in the chain and work backwards. Let’s analyze this exception in that order:

- The lowest exception is an [`IllegalStateException`](https://docs.oracle.com/javase/7/docs/api/java/lang/IllegalStateException.html), since that is the innermost error in our `throwFunc` function. The error occurs when the table tries to compute a value for the `X` column and invokes our function.
- Just above, we see [`RuntimeException`](https://docs.oracle.com/javase/7/docs/api/java/lang/RuntimeException.html) as another 'caused by' statement because the [`IllegalStateException`](https://docs.oracle.com/javase/7/docs/api/java/lang/IllegalStateException.html) was passed as the argument to a [`RuntimeException`](https://docs.oracle.com/javase/7/docs/api/java/lang/RuntimeException.html).
- The top-level [`FormulaEvaluationException`](/core/javadoc/io/deephaven/engine/table/impl/select/FormulaEvaluationException.html) is the point where the Deephaven engine recognized that it had an error it could not handle internally. Any sort of uncaught exception could have been in this position, but a [`FormulaEvaluationException`](/core/javadoc/io/deephaven/engine/table/impl/select/FormulaEvaluationException.html) indicates that some specified formula had a problem. If you had a table with columns `A` and `B`, and you accidentally made a formula referencing column `Aa`, then you would also get a `FormulaEvaluationException`, because the Deephaven engine would be unable to find a column with that exact name.
- Note also that the exception lists the formula as `throwFunc.call(i)`, instead of `throwFunc(i)`, as it was written. This is because the Deephaven engine explicitly uses the `call` syntax when invoking Groovy closures.

This is a very simple example of triaging an equally simple query with deliberate errors. In practice, queries will be much more sophisticated and contain many interdependencies between tables. The recipe for triage, however, is the same.

See the sections below for examples of some common mistakes.

## Common mistakes

Once you've fixed the static problems, your query will start serving up tables and plots. Sometimes the query may crash unexpectedly during its normal runtime. These problems can be trickier to triage because they will often be related to unexpected streaming data patterns.

Your first steps should be the same as for a startup error:

1. Find the exception.
2. Reduce to its set of `caused by` expressions.
3. Start at the last one and work backwards.

The Console in the Deephaven IDE is a critical tool during this process. Once you've isolated the failing operation, you can execute parts of the query bit by bit to analyze the inputs to the failing method and experiment with changes to solve the problem.

Below we will discuss some of the most common categories of failure.

### Unexpected null values

It's easy to forget that columns can contain null values. If your queries don't account for this, they will fail with a [`NullPointerException`](https://docs.oracle.com/javase/7/docs/api/java/lang/NullPointerException.html).

The following shows a simple dataset consisting of a string column with a null value. It attempts to grab strings that start with `B` using the `charAt` method.

```groovy should-fail
table = newTable(stringCol("Items", "A", "B", null, "D"))

result = table.where("Items.charAt(0) == 'B'")
```

In this example, the following information can be found in the last 'Caused by' line of the stack trace:

```none
Caused by: java.lang.NullPointerException: Cannot invoke "String.charAt(int)" because "<local7>" is null
```

As discussed above, using the Console is a good way to inspect the tables for this condition.

In this case, we must check if the value we are trying to inspect is null:

```groovy order=table,result
table = newTable(stringCol("Items", "A", "B", null, "D"))

result = table.where("!isNull(Items) && Items.charAt(0) == 'B'")
```

> [!NOTE]
> Adding [`!isNull(Items)`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#isNull(byte)) to the start of our filter statement works because of boolean short circuiting. In this example, when [`!isNull(Items)`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#isNull(byte)) is `false`, the query will stop evaulating the remainder of the [`where`](../reference/table-operations/filter/where.md) clause, meaning the problematic `Items.charAt(0) == 'B'` statement is never executed on a null value.

### String and array access

Accessing [strings](../reference/query-language/types/strings.md) or [arrays](../reference/query-language/types/arrays.md) may also present difficulties. You may need to classify rows based upon the value of a character within a string of expected size; operations such as [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) will produce cells that contain [arrays](../reference/query-language/types/arrays.md) of values. It's possible you might try to access a value in one of these types that does not exist.

The following example shows a simple dataset consisting of a [string column](../reference/table-operations/create/stringCol.md) with [strings](../reference/query-language/types/strings.md) of varying length. It attempts to grab [strings](../reference/query-language/types/strings.md) that have `i` as the second character.

```groovy should-fail
table = newTable(stringCol("Items", "Ai", "B", "Ci", "Da"))

result = table.where("Items.charAt(1) == 'i'")
```

Like the previous examples, the root cause can be found near the end of the stack trace:

```none {3}
Caused by: java.lang.StringIndexOutOfBoundsException: Index 1 out of bounds for length 1
```

This line contains the actual [query string](./strings.md) that caused the error, making it clear which line is the culprit.

We can inspect the table by adding a new column `ItemsLength = Items.length()`:

```groovy order=length
table = newTable(stringCol("Items", "Ai", "B", "Ci", "Da"))

length = table.update("ItemsLength = Items.length()")
```

To see rows where the string is less than two characters long, filter that table to only rows where `ItemsLength < 2` using the right-click menu results:

![The Advanced Filters panel, set to filter ItemsLength to less than 2](../assets/how-to/errors/triage2.png)

Following the advice above, let's change the query to:

```groovy order=result
table = newTable(stringCol("Items", "Ai", "B", "Ci", "Da"))

result = table.where("Items.length() >= 2 && Items.charAt(1) == 'i'")
```

This works due to Java's boolean short circuiting described above.

If you really only care about the presence of the `i` character in the string, a better solution is:

```groovy order=result
table = newTable(stringCol("Items", "Ai", "B", "Ci", "Da"))
result = table.where("Items.contains(`i`)")
```

As always, use the Deephaven IDE to validate that this change works and produces the result you expect!

### Join key problems

Deephaven's join operations are one of its most powerful features. They merge tables together based upon matching parameters to produce new tables.

Some flavors of join are tolerant to the cardinality of key instances on the left and right hand sides, but some are not. [`naturalJoin`](../reference/table-operations/join/natural-join.md), for example, requires that there is no more than one right hand side key mapped to every left hand side key. This causes issues when the input data to a joined table does not adhere to these requirements. Below is a query that exhibits this behavior.

```groovy should-fail
left = newTable(
	stringCol("LastName", "Rafferty"),
  intCol("DeptID", 31),
  stringCol("Telephone", "(303) 555-0162"),
)
right = newTable(
  intCol("DeptID", 31, 33, 34, 31),
  stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
  stringCol("DeptTelephone", "(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171"),
)

result = left.naturalJoin(right, "DeptID")
```

Because `31` appears twice in the `DeptID` column in the right table, the following error will be thrown:

```none
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: java.lang.IllegalStateException: Natural Join found duplicate right key for 31
```

### Non-deterministic formulas

Not all errors have stack traces. For instance, the use of non-deterministic functions in [formula columns](../how-to-guides/use-select-view-update.md) can lead to undefined results. A deterministic algorithm is one that produces the _exact_ same result every single time when given the same input parameters. A simple example of a non-deterministic function is a random number generator. Consider the following query:

```groovy order=result,source
source = emptyTable(10).updateView("Number = randomInt(1, 10)")

result = source.update("NumberTimesTwo = Number * 2")
```

Notice how the `NumberTimesTwo` column contains values that are all even, but not two times what's in the `Number` column. This is because [`updateView`](../reference/table-operations/select/update-view.md) was used with a random number generator. An [`updateView`](../reference/table-operations/select/update-view.md) creates a formula column, which only stores the formula and calculates values on-demand. So, the values are calculated in `Number`, then re-calculated when computing `NumberTimesTwo`. Thus, `NumberTimesTwo` has two times a completely different random number. Errors like these can be trickier to diagnose, so it's important to be cognizant of what type of columns are created when using any non-deterministic functions.

### Out of memory

One of the most common problems with a query can be a lack of memory (referred to as heap) to handle a query as the data volume grows throughout the day. This occurs if enough heap is not allocated to your Deephaven instance but can be exacerbated by unexpected data patterns, like unusually large Kafka input. Most often this can be fixed by increasing the heap allocated to Deephaven, but an analysis of the query itself may yield improvements to the script that reduces overall heap usage.

<!--TODO: docker ram article https://github.com/deephaven/deephaven.io/issues/577 -->

Below is the most common example an Out Of Heap failure:

`Caused by: java.lang.OutOfMemoryError: Java heap space`

There are many ways to approach this problem and, in practice, resolution will require some combination of all of the techniques mentioned below.

The first, and simplest, action to take is to increase the heap allocated to your Java Virtual Machine. Using the Docker-based example provided by Deephaven in the [Docker install guide](../tutorials/docker-install.md), you could simply increase the amount of memory allocated to Docker, up to certain limits. If you are using the Docker-based example and want to adjust the memory allocation, please see the file `docker-compose-common.yml` in your Deephaven Core base directory.

For many workloads this will be enough. However, users must be aware that you may not get the results you expect when you increase memory across the 32GB boundary. Java uses address compression techniques to optimize memory usage when maximum heap size is smaller than 32GB. Once you request 32GB or more, this feature is disabled so that the program can access its entire memory space. This means that if you start with 30GB of heap and then increase the size to 32GB, you will have less heap available to your query.

> [!TIP]
> If you must cross the 32GB boundary, it is best to jump directly to 42GB to account for this.

The next step is to review your query code.

- Look for places where you use [`update`](../reference/table-operations/select/update.md). Would [`updateView`](../reference/table-operations/select/update-view.md) be better? Keep in mind formula stability. The choice between [`update`](../reference/table-operations/select/update.md) and [`update_view`](../reference/table-operations/select/update-view.md) is a memory vs. CPU tradeoff. You will pay more in CPU time with [`updateView`](../reference/table-operations/select/update-view.md) in order to save on memory. For simple computations and transformations, this is often a very good tradeoff. See our guide, [Choose the right selection method](../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method).
- Search for duplicate tables, or tables that only differ in column count. Try to re-use tables in derivative computations as much as possible to make use of the query's previous work. In most cases, Deephaven automatically recognizes duplicate computations and uses the original tables. However, it is best not to rely on this behavior and instead be explicit about what tables you derive from.
- When using Deephaven query expressions that group rows based on keys ([`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md), [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md), and [`join`](../reference/table-operations/join/join.md) operations), pay close attention to the number of unique keys in the tables you apply these operations to. If keys are generally unique (`SaleID`, for example) within a table, then an operation like [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md) will produce potentially millions of single row tables that all consume some heap space in overhead. In these cases, consider if you can use different keys, or even a different set of Deephaven query operations.
- Carefully consider the order you execute Deephaven query expressions. It’s best to filter data before joining tables.
  - A very simple example is applying a [`join`](../reference/table-operations/join/join.md) and a [`where`](../reference/table-operations/filter/where.md) condition. If you were to execute ``derived = myTable.join(myOtherTable, “JoinColumn”).where(“FilterColumn=`FilterValue`)``, the [`join`](../reference/table-operations/join/join.md) operation consumes much more heap than it needs to, since it will take time matching rows from `myTable` that would later be filtered down in the [`where`](../reference/table-operations/filter/where.md) expression.
  - This query would be better expressed as ``derived = myTable.where(“FilterColumn=`FilterValue`).join(myOtherTable, “JoinColumn”)``. This saves in heap usage, but also reduces how many rows are processed in downstream tables (called ticks) when the left hand or right hand tables tick.

There are many other reasons that a query may use more heap than expected. This requires a deep analysis of how the query is performing at runtime. You may need to use a Java profiling tool (such as [JProfiler](https://www.ej-technologies.com/products/jprofiler/overview.html)) to identify poorly-performing sections of your code.

### Garbage Collection (GC) timeouts

GC timeouts are another type of heap-related problem. These occur when you have enough memory for your query, but lots of data is being manipulated in memory each time your data ticks. This can happen, for example, when performing a long series of query operations. This results in lots of temporary heap allocation to process those upstream ticks. When the upstream computations are done, the JVM can re-collect that memory for use in later calculations. This is called the 'Garbage Collection' phase of JVM processing. Garbage Collection consumes CPU time, which can halt your normal query processing and cause computations to back up even further.

This behavior is characterized by periodic long pauses as the JVM frees up memory. When this happens, look for ways to reduce your tick frequency.

> [!NOTE]
> See [How to reduce the update frequency of ticking tables](./reduce-update-frequency.md)

## Get help

If you've gone through all of the steps to identify the root of a problem, but can’t find a cause in your query, you may have uncovered a bug. In this case, you should file a bug report at https://github.com/deephaven/deephaven-core/issues. Be sure to include the following with your bug report:

- The version of the Deephaven system.
- The complete stack trace, not just the list of `Caused By` expressions.
- A code snippet or a description of the query.
- Any logs from your user console or, if running Docker, from your terminal.
- Support logs exported from the browser. In the Settings menu, click the Export Logs button to download a zip with the browser logs and the current state of the application.

You can also get help by asking questions in our [GitHub Discussions](https://github.com/deephaven/deephaven-core/discussions/categories/q-a) forum.

## Appendix: Common errors in queries

1. Is your Deephaven query expression properly quoted?

- Expressions within Deephaven query statements such as [`where`](../reference/table-operations/filter/where.md) or [`update`](../reference/table-operations/select/update.md) must be surrounded by double quotes. For example, ``myTable.where("StringColumn=`TheValue`")``.
- If you copy-pasted an expression from somewhere (e.g., Slack), some systems will copy a unicode double quotation (U+201C and U+201D) instead of an ASCII quotation (U+0022).

2. Do you have matching parentheses?

   - Make sure that any open parenthesis that you are using are matched by a close parenthesis.

3. If you are referring to a column in a Deephaven query expression:

   - Check the spelling and capitalization.
   - Is the type of the column what you expect it to be?
   - Did you account for null values?

4. If you are using strings in your expression, did you quote them properly?

   - Strings are quoted with the backtick character (`` ` ``), **not** single quotes (`‘`) or double quotes (`"`).
   - Do you have matching close quotes for your open quotes?

5. If you are using date-times in your expressions:

   - Did you use single quotes (`‘`), not double quotes (`“`) or backticks (`` ` ``)?
   - Did you use the proper format? Deephaven date-times are expected as `<yyyyMMDD>T<HH:mm:ss.nnnnnnnnn> <TZ>`.

6. Are all classes that you are trying to use properly imported?

   - will not search for classes Groovy that you use. If they are not part of the [standard set of Deephaven imports](https://github.com/deephaven/deephaven-core/blob/main/engine/table/src/main/java/io/deephaven/engine/table/lang/impl/QueryLibraryImportsDefaults.java), you must import them yourself.

## Related documentation

- [Create a new table](./new-and-empty-table.md#newtable)
- [How to handle null, infinity, and not-a-number values](./null-inf-nan.md)
- [Joins: Exact and Relational](./joins-exact-relational.md)
- [Joins: Time-Series and Range](./joins-timeseries-range.md)
- [How to select, view, and update data in tables](./use-select-view-update.md)
- [How to work with strings](./strings.md)
- [Formulas](../how-to-guides/formulas.md)
- [Special variables](../reference/query-language/variables/special-variables.md)
