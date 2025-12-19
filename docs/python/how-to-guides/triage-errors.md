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

```python should-fail
from deephaven import empty_table


def throw_func(val):
    raise RuntimeError("Error: " + val)


x = empty_table(1).update(formulas=["X = throw_func(i)"])
```

<details className="error">
<summary>The query fails with the following error: </summary>

```
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: java.lang.RuntimeException: Error in Python interpreter:
Type: <class 'deephaven.dherror.DHError'>
Value: table update operation failed. : Value: Error: 0
Traceback (most recent call last):
  File "/opt/deephaven/venv/lib/python3.10/site-packages/deephaven/table.py", line 764, in update
    return Table(j_table=self.j_table.update(*formulas))
RuntimeError: io.deephaven.engine.exceptions.TableInitializationException: Error while initializing Update([X]): an exception occurred while performing the initial select or update
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$32(QueryTable.java:1563)
	at io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder.withNugget(QueryPerformanceRecorder.java:369)
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$33(QueryTable.java:1500)
	at io.deephaven.engine.table.impl.QueryTable.memoizeResult(QueryTable.java:3639)
	at io.deephaven.engine.table.impl.QueryTable.selectOrUpdate(QueryTable.java:1499)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:1477)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:100)
	at io.deephaven.api.TableOperationsDefaults.update(TableOperationsDefaults.java:94)
	at org.jpy.PyLib.executeCode(Native Method)
	at org.jpy.PyObject.executeCode(PyObject.java:138)
	at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
	at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:205)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:51)
	at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:205)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$0(AbstractScriptSession.java:163)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:196)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:207)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:195)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:163)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:72)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:75)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$4(ConsoleServiceGrpcImpl.java:191)
	at io.deephaven.server.session.SessionState$ExportBuilder.lambda$submit$2(SessionState.java:1536)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:995)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.server.runner.scheduler.SchedulerModule$ThreadFactory.lambda$newThread$0(SchedulerModule.java:100)
	at java.base/java.lang.Thread.run(Thread.java:1583)
caused by java.lang.RuntimeException: Error in Python interpreter:
Type: <class 'RuntimeError'>
Value: Error: 0
Line: 5
Namespace: throw_func
File: <string>
Traceback (most recent call last):
  File "/opt/deephaven/venv/lib/python3.10/site-packages/deephaven/_udf.py", line 594, in _vectorization_wrapper
  File "<string>", line 5, in throw_func

	at org.jpy.PyLib.callAndReturnValue(Native Method)
	at org.jpy.PyObject.call(PyObject.java:474)
	at io.deephaven.engine.table.impl.select.python.FormulaKernelPythonChunkedFunction.applyFormulaChunk(FormulaKernelPythonChunkedFunction.java:147)
	at io.deephaven.engine.table.impl.select.FormulaKernelTypedBase$ToTypedMethod.visit(FormulaKernelTypedBase.java:174)
	at io.deephaven.chunk.ObjectChunk.walk(ObjectChunk.java:129)
	at io.deephaven.engine.table.impl.select.FormulaKernelTypedBase.applyFormulaChunk(FormulaKernelTypedBase.java:42)
	at io.deephaven.engine.table.impl.select.formula.FormulaKernelAdapter.fillChunkHelper(FormulaKernelAdapter.java:299)
	at io.deephaven.engine.table.impl.select.formula.FormulaKernelAdapter.fillChunk(FormulaKernelAdapter.java:229)
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
	... 29 more


Line: 766
Namespace: update
File: /opt/deephaven/venv/lib/python3.10/site-packages/deephaven/table.py
Traceback (most recent call last):
  File "<string>", line 8, in <module>
  File "/opt/deephaven/venv/lib/python3.10/site-packages/deephaven/table.py", line 766, in update

	at org.jpy.PyLib.executeCode(Native Method)
	at org.jpy.PyObject.executeCode(PyObject.java:138)
	at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
	at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:205)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:51)
	at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:205)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$0(AbstractScriptSession.java:163)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:196)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:207)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:195)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:163)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:72)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:75)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$4(ConsoleServiceGrpcImpl.java:191)
	at io.deephaven.server.session.SessionState$ExportBuilder.lambda$submit$2(SessionState.java:1536)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:995)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.server.runner.scheduler.SchedulerModule$ThreadFactory.lambda$newThread$0(SchedulerModule.java:100)
	at java.base/java.lang.Thread.run(Thread.java:1583)
```

</details>

Deephaven stack traces tend to be very large, but lead off with a summation of the underlying error. Look at the first few lines of the above stack trace:

```none {2,4}
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: java.lang.RuntimeException: Error in Python interpreter:
Type: <class 'deephaven.dherror.DHError'>
Value: table update operation failed. : Value: Error: 0
Traceback (most recent call last):
  File "/opt/deephaven/venv/lib/python3.10/site-packages/deephaven/table.py", line 764, in update
    return Table(j_table=self.j_table.update(*formulas))
RuntimeError: io.deephaven.engine.exceptions.TableInitializationException: Error while initializing Update([X]): an exception occurred while performing the initial select or update
```

There are two important lines from this error message:

- `Value: table update operation failed. : Value: Error: 0`
  - This line contains the value error the function was told to raise.
- `RuntimeError: io.deephaven.engine.exceptions.TableInitializationException: Error while initializing Update([X]): an exception occurred while performing the initial select or update`
  - This tells us the error was made during an [`update`](../reference/table-operations/select/update.md) operation.
  - This also tells us that the column name that caused the error is `X`.

Scroll past the first block of indented lines saying `at io.deephaven...`; there's some more important information:

```none {2, 4}
caused by java.lang.RuntimeException: Error in Python interpreter:
Type: <class 'RuntimeError'>
Value: Error: 0
Line: 5
Namespace: throw_func
File: <string>
Traceback (most recent call last):
  File "/opt/deephaven/venv/lib/python3.10/site-packages/deephaven/_udf.py", line 594, in _vectorization_wrapper
  File "<string>", line 5, in throw_func
```

Most lines in the above block of stack trace indicate what's going on, but these two point to _exactly_ where the error happens:

- `Line: 5`
- `File "<string>", line 5, in throw_func`

Both of these tell us that the query errored on line 5, which is the same line where the `RuntimeError` gets thrown.

When triaging errors in Deephaven Python queries, there are a few rules of thumb to follow that can help make it easier:

- The important details are _rarely_ at the bottom of the stack trace.
- The first few lines, like above, have information that can be used to identify the cause.
- The important information is typically in blocks of text that are _not_ indented. Indented text points to the underlying Java.

This is a very simple example of triaging an equally simple query with deliberate errors. In practice, queries will be much more sophisticated and contain many interdependencies between tables. The recipe for triage, however, is the same.

See the sections below for examples of some common mistakes.

## Common mistakes

Once you've fixed the static problems, your query will start serving up tables and plots. Sometimes the query may crash unexpectedly during its normal runtime. These problems can be trickier to triage because they will often be related to unexpected streaming data patterns.

Your first steps should be the same as for a startup error:

1. Start at the top.
2. Skim over indented blocks of text.
3. Look for Python-specific exceptions.

The Console in the Deephaven IDE is a critical tool during this process. Once you've isolated the failing operation, you can execute parts of the query bit by bit to analyze the inputs to the failing method and experiment with changes to solve the problem.

Below we will discuss some of the most common categories of failure.

### Unexpected null values

It's easy to forget that columns can contain null values. If your queries don't account for this, they will fail with a [`NullPointerException`](https://docs.oracle.com/javase/7/docs/api/java/lang/NullPointerException.html).

The following shows a simple dataset consisting of a string column with a null value. It attempts to grab strings that start with `B` using the `charAt` method.

```python should-fail
from deephaven import new_table
from deephaven.column import string_col

table = new_table([string_col("Items", ["A", "B", None, "D"])])

result = table.where(filters=["Items.charAt(0) == 'B'"])
```

In this example, the following information can be found in the first few lines of the stack trace:

```none
Value: table where operation failed. : java.lang.NullPointerException: Cannot invoke "String.charAt(int)" because "<local7>" is null
```

As discussed above, using the Console is a good way to inspect the tables for this condition.

In this case, we must check if the value we are trying to inspect is null:

```python order=table,result
from deephaven import new_table
from deephaven.column import string_col

table = new_table([string_col("Items", ["A", "B", None, "D"])])

result = table.where(filters=["!isNull(Items) && Items.charAt(0) == 'B'"])
```

> [!NOTE]
> Adding `!isNull(Items)` to the start of our filter statement works because of boolean short circuiting. In this example, when `!isNull(Items)` is `false`, the query will stop evaulating the remainder of the [`where`](../reference/table-operations/filter/where.md) clause, meaning the problematic `Items.charAt(0) == 'B'` statement is never executed on a null value.

### String and array access

Accessing [strings](../reference/query-language/types/strings.md) or [arrays](../reference/query-language/types/arrays.md) may also present difficulties. You may need to classify rows based upon the value of a character within a string of expected size; operations such as [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) will produce cells that contain [arrays](../reference/query-language/types/arrays.md) of values. It's possible you might try to access a value in one of these types that does not exist.

The following example shows a simple dataset consisting of a [string column](../reference/table-operations/create/stringCol.md) with [strings](../reference/query-language/types/strings.md) of varying length. It attempts to grab [strings](../reference/query-language/types/strings.md) that have `i` as the second character.

```python should-fail
from deephaven import new_table
from deephaven.column import string_col

table = new_table([string_col("Items", ["Ai", "B", "Ci", "Da"])])

result = table.where(filters=["Items.charAt(1) == 'i'"])
```

Like the previous examples, the root cause can be found in the first few lines of the stack trace:

```none {3}
RuntimeError: io.deephaven.engine.exceptions.TableInitializationException: Error while initializing where([Items.charAt(1) == 'i']): an exception occurred while performing the initial filter
```

This line contains the actual [query string](./work-with-strings.md) that caused the error, making it clear which line is the culprit.

We can inspect the table by adding a new column `ItemsLength = Items.length()`:

```python order=length
from deephaven import new_table
from deephaven.column import string_col

table = new_table([string_col("Items", ["Ai", "B", "Ci", "Da"])])

length = table.update(formulas=["ItemsLength = Items.length()"])
```

To see rows where the string is less than two characters long, filter that table to only rows where `ItemsLength < 2` using the right-click menu results:

![The Advanced Filters panel, set to filter ItemsLength to less than 2](../assets/how-to/errors/triage2.png)

Following the advice above, let's change the query to:

```python order=result
from deephaven import new_table
from deephaven.column import string_col

table = new_table([string_col("Items", ["Ai", "B", "Ci", "Da"])])

result = table.where(filters=["Items.length() >= 2 && Items.charAt(1) == 'i'"])
```

This works due to Java's boolean short circuiting described above.

If you really only care about the presence of the `i` character in the string, a better solution is:

```python order=result
from deephaven import new_table
from deephaven.column import string_col

table = new_table([string_col("Items", ["Ai", "B", "Ci", "Da"])])
result = table.where(filters=["Items.contains(`i`)"])
```

As always, use the Deephaven IDE to validate that this change works and produces the result you expect!

### Join key problems

Deephaven's join operations are one of its most powerful features. They merge tables together based upon matching parameters to produce new tables.

Some flavors of join are tolerant to the cardinality of key instances on the left and right hand sides, but some are not. [`natural_join`](../reference/table-operations/join/natural-join.md), for example, requires that there is no more than one right hand side key mapped to every left hand side key. This causes issues when the input data to a joined table does not adhere to these requirements. Below is a query that exhibits this behavior.

```python should-fail
from deephaven import new_table

from deephaven.column import string_col, int_col

left = new_table(
    [
        string_col("LastName", ["Rafferty"]),
        int_col("DeptID", [31]),
        string_col("Telephone", ["(303) 555-0162"]),
    ]
)
right = new_table(
    [
        int_col("DeptID", [31, 33, 34, 31]),
        string_col("DeptName", ["Sales", "Engineering", "Clerical", "Marketing"]),
        string_col(
            "DeptTelephone",
            ["(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171"],
        ),
    ]
)

result = left.natural_join(right, "DeptID")
```

Because `31` appears twice in the `DeptID` column in the right table, the following error will be thrown:

```none
Value: table natural_join operation failed. : RuntimeError: java.lang.IllegalStateException: Natural Join found duplicate right key for 31
```

### Non-deterministic formulas

Not all errors have stack traces. For instance, the use of non-deterministic functions in [formula columns](../how-to-guides/use-select-view-update.md) can lead to undefined results. A deterministic algorithm is one that produces the _exact_ same result every single time when given the same input parameters. A simple example of a non-deterministic function is a random number generator. Consider the following query:

```python order=result,source
from deephaven import empty_table

source = empty_table(10).update_view("Number = randomInt(1, 10)")

result = source.update("NumberTimesTwo = Number * 2")
```

Notice how the `NumberTimesTwo` column contains values that are all even, but not two times what's in the `Number` column. This is because [`update_view`](../reference/table-operations/select/update-view.md) was used with a random number generator. An [`update_view`](../reference/table-operations/select/update-view.md) creates a formula column, which only stores the formula and calculates values on-demand. So, the values are calculated in `Number`, then re-calculated when computing `NumberTimesTwo`. Thus, `NumberTimesTwo` has two times a completely different random number. Errors like these can be trickier to diagnose, so it's important to be cognizant of what type of columns are created when using any non-deterministic functions.

### Out of memory

One of the most common problems with a query can be a lack of memory (referred to as heap) to handle a query as the data volume grows throughout the day. This occurs if enough heap is not allocated to your Deephaven instance but can be exacerbated by unexpected data patterns, like unusually large Kafka input. Most often this can be fixed by increasing the heap allocated to Deephaven, but an analysis of the query itself may yield improvements to the script that reduces overall heap usage.

<!--TODO: docker ram article https://github.com/deephaven/deephaven.io/issues/577 -->

Below is the most common example of Out Of Heap failures:

`Caused by: java.lang.OutOfMemoryError: Java heap space`

There are many ways to approach this problem and, in practice, resolution will require some combination of all of the techniques mentioned below.

The first, and simplest, action to take is to increase the heap allocated to your Java Virtual Machine. Using the Docker-based example provided by Deephaven in the [Docker install guide](../getting-started/docker-install.md), you could simply increase the amount of memory allocated to Docker, up to certain limits. If you are using the Docker-based example and want to adjust the memory allocation, please see the file `docker-compose-common.yml` in your Deephaven Core base directory.

For many workloads this will be enough. However, users must be aware that you may not get the results you expect when you increase memory across the 32GB boundary. Java uses address compression techniques to optimize memory usage when maximum heap size is smaller than 32GB. Once you request 32GB or more, this feature is disabled so that the program can access its entire memory space. This means that if you start with 30GB of heap and then increase the size to 32GB, you will have less heap available to your query.

> [!TIP]
> If you must cross the 32GB boundary, it is best to jump directly to 42GB to account for this.

The next step is to review your query code.

- Look for places where you use [`update`](../reference/table-operations/select/update.md). Would [`update_view`](../reference/table-operations/select/update-view.md) be better? Keep in mind formula stability. The choice between [`update`](../reference/table-operations/select/update.md) and [`update_view`](../reference/table-operations/select/update-view.md) is a memory vs. CPU tradeoff. You will pay more in CPU time with [`update_view`](../reference/table-operations/select/update-view.md) in order to save on memory. For simple computations and transformations, this is often a very good tradeoff. See our guide, [Choose the right selection method](../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method).
- Search for duplicate tables, or tables that only differ in column count. Try to re-use tables in derivative computations as much as possible to make use of the query's previous work. In most cases, Deephaven automatically recognizes duplicate computations and uses the original tables. However, it is best not to rely on this behavior and instead be explicit about what tables you derive from.
- When using Deephaven query expressions that group rows based on keys ([`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md), [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md), and [`join`](../reference/table-operations/join/join.md) operations), pay close attention to the number of unique keys in the tables you apply these operations to. If keys are generally unique (`SaleID`, for example) within a table, then an operation like [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md) will produce potentially millions of single row tables that all consume some heap space in overhead. In these cases, consider if you can use different keys, or even a different set of Deephaven query operations.
- Carefully consider the order you execute Deephaven query expressions. It’s best to filter data before joining tables.
  - A very simple example is applying a [`join`](../reference/table-operations/join/join.md) and a [`where`](../reference/table-operations/filter/where.md) condition. If you were to execute ``derived = myTable.join(myOtherTable, “JoinColumn”).where(“FilterColumn=`FilterValue`)``, the [`join`](../reference/table-operations/join/join.md) operation consumes much more heap than it needs to, since it will take time matching rows from `myTable` that would later be filtered down in the [`where`](../reference/table-operations/filter/where.md) expression.
  - This query would be better expressed as ``derived = myTable.where(“FilterColumn=`FilterValue`).join(myOtherTable, “JoinColumn”)``. This saves in heap usage, but also reduces how many rows are processed in downstream tables (called ticks) when the left hand or right hand tables tick.

There are many other reasons that a query may use more heap than expected. This requires a deep analysis of how the query is performing at runtime. You may need to use a Java profiling tool (such as [JProfiler](https://www.ej-technologies.com/products/jprofiler/overview.html)) to identify poorly-performing sections of your code.

### Garbage Collection (GC) timeouts

GC timeouts are another type of heap-related problem. These occur when you have enough memory for your query, but lots of data is being manipulated in memory each time your data ticks. This can happen, for example, when performing a long series of query operations. This results in lots of temporary heap allocation to process those upstream ticks. When the upstream computations are done, the JVM can re-collect that memory for use in later calculations. This is called the 'Garbage Collection' phase of JVM processing. Garbage Collection consumes CPU time, which can halt your normal query processing and cause computations to back up even further.

This behavior is characterized by periodic long pauses as the JVM frees up memory. When this happens, look for ways to reduce your tick frequency.

> [!NOTE]
> See [How to reduce the update frequency of ticking tables](./performance/reduce-update-frequency.md)

## Get help

If you've gone through all of the steps to identify the root of a problem, but can’t find a cause in your query, you may have uncovered a bug. In this case, you should file a [bug report](https://github.com/deephaven/deephaven-core/issues). Be sure to include the following with your bug report:

- The version of the Deephaven system.
- The complete stack trace, not just the list of `Caused By` expressions.
- A code snippet or a description of the query.
- Any logs from your user console or, if running Docker, from your terminal.
- Support logs exported from the browser. In the Settings menu, click the Export Logs button to download a zip with the browser logs and the current state of the application.

You can also get help by asking questions in our [GitHub Discussions](https://github.com/deephaven/deephaven-core/discussions/categories/q-a) forum.

## Appendix: Common errors in queries

1. Is your Deephaven query expression properly quoted?

- Expressions within Deephaven query statements such as [`where`](../reference/table-operations/filter/where.md) or [`update`](../reference/table-operations/select/update.md) must be surrounded by double quotes. For example, ``myTable.where(filters=["StringColumn=`TheValue`"])``.
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

- [Create a new table](./new-and-empty-table.md#new_table)
- [How to handle null, infinity, and not-a-number values](./null-inf-nan.md)
- [Joins: Exact and Relational](./joins-exact-relational.md)
- [Joins: Time-Series and Range](./joins-timeseries-range.md)
- [How to select, view, and update data in tables](./use-select-view-update.md)
- [How to work with strings](./work-with-strings.md)
- [Filters in query strings](./filters.md)
- [Formulas in query strings](./formulas.md)
- [Special variables](../reference/query-language/variables/special-variables.md)
