---
title: Decipher Python errors
---

When you develop Python queries in Deephaven, you eventually write code that causes an error. Deephaven's error output combines a Python traceback with Java stack traces from the query engine, so it can be long. This guide shows you how to read three kinds of errors so that you can quickly find and fix problems:

- Errors from code you run in the console.
- Errors from [table operations](../conceptual/table-operations-overview.md) on [static](../conceptual/table-types.md#static-tables) and [ticking tables](../conceptual/table-types.md#standard-streaming-tables).
- [Query strings](./query-string-overview.md) that fail to compile.

For a short checklist, see [Tips for reading errors](#tips-for-reading-errors).

## Parts of an error message

Errors in Python come in two kinds: syntax errors and exceptions. Let's start with a syntax error, which occurs when the Python interpreter encounters invalid syntax:

```python skip-test
x === 9
```

<details className="error">
<summary> This produces some scary-looking output: </summary>

```
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: java.lang.RuntimeException: Error in Python interpreter:
  File "<string>", line 1
    x === 9
        ^
SyntaxError: invalid syntax

	at org.jpy.PyLib.executeCode(Native Method)
	at org.jpy.PyObject.executeCode(PyObject.java:172)
	at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
	at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:229)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:51)
	at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:229)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$0(AbstractScriptSession.java:169)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:214)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:225)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:213)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:169)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:77)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:123)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$7(ConsoleServiceGrpcImpl.java:204)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:1244)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.server.runner.scheduler.SchedulerModule$ThreadFactory.lambda$newThread$0(SchedulerModule.java:100)
	at org.jpy.PyLib.callAndReturnObject(Native Method)
	at org.jpy.PyObject.call(PyObject.java:607)
	at io.deephaven.server.console.python.DebuggingInitializer.lambda$createInitializer$0(DebuggingInitializer.java:46)
	at java.base/java.lang.Thread.run(Thread.java:1583)
```

</details>

This output contains the clues you need to track down what is wrong with your program. In the IDE's [Log panel](./logs.md#log-panel), it has three parts:

1. A log header that names the thread and the part of Deephaven that reported the error. `Error running script` means the error happened while the console ran your code.
2. The Python error. For an exception raised while your code runs, this is a traceback. Each `File` line in the traceback is a frame that gives the file, line, and function of one call that led to the error. For a syntax error like this one, the `File` line gives the location of the invalid code instead. The last line gives the error type and message.
3. A Java stack trace that lists the internal Java calls that led to the error. Each of its lines starts with `at`.

The IDE console shows the same error without the log header. The IDE's [Log panel](./logs.md#log-panel) shows the time and the log level, such as `ERROR`, before the header. In [Docker](../getting-started/docker-install.md) output or the terminal of a server you [started with Gradle](../getting-started/launch-build.md), each log line starts with the UTC date and time, and the log level follows the thread name.

The log shortens the thread and class names in the header to fit fixed-width columns:

- The thread name keeps its last 20 characters. For example, `r-Scheduler-Serial-1` is the end of `DeephavenApiServer-Scheduler-Serial-1`, the server thread that runs console commands.
- The class name keeps its last 25 characters, after the log shortens package names to their initials. For example, `.c.ConsoleServiceGrpcImpl` is the end of `i.d.s.c.ConsoleServiceGrpcImpl`, the server class that handles console requests.

> [!NOTE]
> Because the code ran as a console command, the file is `<string>`. Code you run from a [notebook](./user-interface/file-explorer.md) in the IDE also reports `<string>`. When the code comes from a file that Python loads itself, such as a module you import, the traceback shows that file's path instead.

In this case, the Python error shows that a `SyntaxError` occurred on line 1, the only line of our program. The caret (`^`) marks where Python stopped parsing the line.

The Java stack trace shows 24 lines of internal Java calls from Deephaven, jpy (the library that connects Python and Java in Deephaven), and the JDK. In most cases, you can ignore these internal details. As you work through a few errors, you learn to tell which parts of the output are _your_ code and which parts are internal Deephaven code.

## Exceptions in your own functions

Exceptions are unexpected events that occur during a program's execution. Now let's create our own exception. Here we create a function `f(x)` that raises an exception when `x` is 10 or 20.

```python skip-test
def f(x: int) -> int:
    if x == 10:
        raise Exception("oops 1")
    elif x == 20:
        raise Exception("oops 2")
    else:
        return 1


f(1)
f(10)
```

<details className="error">
<summary> Click to see the exception. </summary>

```
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: java.lang.RuntimeException: Error in Python interpreter:
Traceback (most recent call last):
  File "<string>", line 11, in <module>
  File "<string>", line 3, in f
Exception: oops 1

	at org.jpy.PyLib.executeCode(Native Method)
	at org.jpy.PyObject.executeCode(PyObject.java:172)
	at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
	at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:229)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:51)
	at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:229)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$0(AbstractScriptSession.java:169)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:214)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:225)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:213)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:169)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:77)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:123)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$7(ConsoleServiceGrpcImpl.java:204)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:1244)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.server.runner.scheduler.SchedulerModule$ThreadFactory.lambda$newThread$0(SchedulerModule.java:100)
	at org.jpy.PyLib.callAndReturnObject(Native Method)
	at org.jpy.PyObject.call(PyObject.java:607)
	at io.deephaven.server.console.python.DebuggingInitializer.lambda$createInitializer$0(DebuggingInitializer.java:46)
	at java.base/java.lang.Thread.run(Thread.java:1583)
```

</details>

Here we see that the error is an `Exception` with the error message `oops 1`. The last line of the traceback gives the type and the message:

```
Exception: oops 1
```

The two `File` lines above it are the frames, most recent call last:

```
Traceback (most recent call last):
  File "<string>", line 11, in <module>
  File "<string>", line 3, in f
```

In this case, the traceback is helpful. It shows that the error occurred when the program executed line 11 (`f(10)`) and then line 3 (`raise Exception("oops 1")`). `in <module>` marks top-level code in the command you ran, and `in f` marks code inside the function `f`. Calling `f(1)` did not cause an error. Only `f(10)` did.

Now that you have the hang of this, let's make the example a little more difficult. The function `g` below calls `f` from the previous example, so run that definition first.

```python skip-test
def g() -> int:
    x = 0
    x += f(0)
    x += f(3)
    x += f(10)
    return x


g()
```

<details className="error">
<summary> Here you can see that line 5 in `g` (`x += f(10)`) triggered the exception in `f`: </summary>

```
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: java.lang.RuntimeException: Error in Python interpreter:
Traceback (most recent call last):
  File "<string>", line 9, in <module>
  File "<string>", line 5, in g
  File "<string>", line 3, in f
Exception: oops 1

	at org.jpy.PyLib.executeCode(Native Method)
	at org.jpy.PyObject.executeCode(PyObject.java:172)
	at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
	at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:229)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:51)
	at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:229)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$0(AbstractScriptSession.java:169)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:214)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:225)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:213)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:169)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:77)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:123)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$7(ConsoleServiceGrpcImpl.java:204)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:1244)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.server.runner.scheduler.SchedulerModule$ThreadFactory.lambda$newThread$0(SchedulerModule.java:100)
	at org.jpy.PyLib.callAndReturnObject(Native Method)
	at org.jpy.PyObject.call(PyObject.java:607)
	at io.deephaven.server.console.python.DebuggingInitializer.lambda$createInitializer$0(DebuggingInitializer.java:46)
	at java.base/java.lang.Thread.run(Thread.java:1583)
```

</details>

Line numbers in a `File "<string>"` frame count from the start of the console command that defined the code. Here, `line 9, in <module>` and `line 5, in g` count from the start of the command that defined `g`, while `line 3, in f` counts from the start of the earlier command that defined `f`.

## Exceptions in Deephaven tables

You can apply [Python functions](./python-functions.md) to Deephaven tables, and those functions can raise errors too. The first two examples in this section call the function `f` from [Exceptions in your own functions](#exceptions-in-your-own-functions), which raises an exception when its input is 10 or 20. Run that definition first.

Every example in this section calls Python from a [query string](./query-string-overview.md): a string passed to a [table operation](../conceptual/table-operations-overview.md) to [create a column](./formulas.md) or [filter rows](./filters.md), such as `"Y = f(X)"` in the call to [`update`](../reference/table-operations/select/update.md).

### Static tables

Here an error occurs when the query adds a new column to a [static table](../conceptual/table-types.md#static-tables). Because the table is static, Deephaven performs every calculation when your code calls [`update`](../reference/table-operations/select/update.md).

```python skip-test
from deephaven import empty_table

t = empty_table(20).update("X = i")
t1 = t.update("Y = f(X)")
```

<details className="error">
<summary> This error message is far more complex: </summary>

```
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: java.lang.RuntimeException: Error in Python interpreter:
Traceback (most recent call last):
  File "/opt/deephaven/venv/lib/python3.12/site-packages/deephaven/table.py", line 1372, in update
    return Table(j_table=self.j_table.update(*formulas))
                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
RuntimeError: io.deephaven.engine.exceptions.TableInitializationException: Error while initializing Update([Y]): an exception occurred while performing the initial select or update
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$38(QueryTable.java:1898)
	at io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder.withNugget(QueryPerformanceRecorder.java:395)
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$39(QueryTable.java:1835)
	at io.deephaven.engine.table.impl.QueryTable.memoizeResult(QueryTable.java:3357)
	at io.deephaven.engine.table.impl.QueryTable.selectOrUpdate(QueryTable.java:1834)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:1813)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:94)
	at io.deephaven.api.TableOperationsDefaults.update(TableOperationsDefaults.java:94)
	at org.jpy.PyLib.executeCode(Native Method)
	at org.jpy.PyObject.executeCode(PyObject.java:172)
	at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
	at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:229)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:51)
	at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:229)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$0(AbstractScriptSession.java:169)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:214)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:225)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:213)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:169)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:77)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:123)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$7(ConsoleServiceGrpcImpl.java:204)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:1244)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.server.runner.scheduler.SchedulerModule$ThreadFactory.lambda$newThread$0(SchedulerModule.java:100)
	at org.jpy.PyLib.callAndReturnObject(Native Method)
	at org.jpy.PyObject.call(PyObject.java:607)
	at io.deephaven.server.console.python.DebuggingInitializer.lambda$createInitializer$0(DebuggingInitializer.java:46)
	at java.base/java.lang.Thread.run(Thread.java:1583)
caused by java.lang.RuntimeException: Error in Python interpreter:
Traceback (most recent call last):
  File "<string>", line 7, in _wrapper
  File "<string>", line 3, in f
Exception: oops 1

	at org.jpy.PyLib.callAndReturnValue(Native Method)
	at org.jpy.PyObject.call(PyObject.java:636)
	at io.deephaven.engine.table.impl.select.python.FormulaKernelPythonChunkedFunction.applyFormulaChunk(FormulaKernelPythonChunkedFunction.java:106)
	at io.deephaven.engine.table.impl.select.FormulaKernelTypedBase$ToTypedMethod.visit(FormulaKernelTypedBase.java:147)
	at io.deephaven.chunk.LongChunk.walk(LongChunk.java:136)
	at io.deephaven.engine.table.impl.select.FormulaKernelTypedBase.applyFormulaChunk(FormulaKernelTypedBase.java:42)
	at io.deephaven.engine.table.impl.select.formula.FormulaKernelAdapter.fillChunkHelper(FormulaKernelAdapter.java:299)
	at io.deephaven.engine.table.impl.select.formula.FormulaKernelAdapter.fillChunk(FormulaKernelAdapter.java:229)
	at io.deephaven.engine.table.impl.sources.ViewColumnSource.fillChunk(ViewColumnSource.java:219)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doApplyUpdate(SelectColumnLayer.java:466)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.lambda$doSerialApplyUpdate$7(SelectColumnLayer.java:312)
	at io.deephaven.engine.util.systemicmarking.SystemicObjectTracker.executeSystemically(SystemicObjectTracker.java:66)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doSerialApplyUpdate(SelectColumnLayer.java:311)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.lambda$createUpdateHandler$2(SelectColumnLayer.java:249)
	at io.deephaven.engine.table.impl.util.OperationInitializerJobScheduler.wrapRunnable(OperationInitializerJobScheduler.java:71)
	at io.deephaven.engine.table.impl.util.OperationInitializerJobScheduler.lambda$submit$0(OperationInitializerJobScheduler.java:42)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:214)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:225)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:213)
	at io.deephaven.engine.table.impl.OperationInitializationThreadPool$1.lambda$newThread$0(OperationInitializationThreadPool.java:50)
	... 4 more


The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "<string>", line 4, in <module>
  File "/opt/deephaven/venv/lib/python3.12/site-packages/deephaven/table.py", line 1374, in update
    raise DHError(e, "table update operation failed.") from e
deephaven.dherror.DHError: table update operation failed. : java.lang.RuntimeException: Error in Python interpreter:
Traceback (most recent call last):
  File "/opt/deephaven/venv/lib/python3.12/site-packages/deephaven/table.py", line 1372, in update
    return Table(j_table=self.j_table.update(*formulas))
                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
RuntimeError: io.deephaven.engine.exceptions.TableInitializationException: Error while initializing Update([Y]): an exception occurred while performing the initial select or update
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$38(QueryTable.java:1898)
	at io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder.withNugget(QueryPerformanceRecorder.java:395)
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$39(QueryTable.java:1835)
	at io.deephaven.engine.table.impl.QueryTable.memoizeResult(QueryTable.java:3357)
	at io.deephaven.engine.table.impl.QueryTable.selectOrUpdate(QueryTable.java:1834)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:1813)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:94)
	at io.deephaven.api.TableOperationsDefaults.update(TableOperationsDefaults.java:94)
	at org.jpy.PyLib.executeCode(Native Method)
	at org.jpy.PyObject.executeCode(PyObject.java:172)
	at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
	at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:229)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:51)
	at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:229)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$0(AbstractScriptSession.java:169)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:214)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:225)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:213)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:169)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:77)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:123)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$7(ConsoleServiceGrpcImpl.java:204)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:1244)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.server.runner.scheduler.SchedulerModule$ThreadFactory.lambda$newThread$0(SchedulerModule.java:100)
	at org.jpy.PyLib.callAndReturnObject(Native Method)
	at org.jpy.PyObject.call(PyObject.java:607)
	at io.deephaven.server.console.python.DebuggingInitializer.lambda$createInitializer$0(DebuggingInitializer.java:46)
	at java.base/java.lang.Thread.run(Thread.java:1583)
caused by java.lang.RuntimeException: Error in Python interpreter:
Traceback (most recent call last):
  File "<string>", line 7, in _wrapper
  File "<string>", line 3, in f
Exception: oops 1

	at org.jpy.PyLib.callAndReturnValue(Native Method)
	at org.jpy.PyObject.call(PyObject.java:636)
	at io.deephaven.engine.table.impl.select.python.FormulaKernelPythonChunkedFunction.applyFormulaChunk(FormulaKernelPythonChunkedFunction.java:106)
	at io.deephaven.engine.table.impl.select.FormulaKernelTypedBase$ToTypedMethod.visit(FormulaKernelTypedBase.java:147)
	at io.deephaven.chunk.LongChunk.walk(LongChunk.java:136)
	at io.deephaven.engine.table.impl.select.FormulaKernelTypedBase.applyFormulaChunk(FormulaKernelTypedBase.java:42)
	at io.deephaven.engine.table.impl.select.formula.FormulaKernelAdapter.fillChunkHelper(FormulaKernelAdapter.java:299)
	at io.deephaven.engine.table.impl.select.formula.FormulaKernelAdapter.fillChunk(FormulaKernelAdapter.java:229)
	at io.deephaven.engine.table.impl.sources.ViewColumnSource.fillChunk(ViewColumnSource.java:219)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doApplyUpdate(SelectColumnLayer.java:466)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.lambda$doSerialApplyUpdate$7(SelectColumnLayer.java:312)
	at io.deephaven.engine.util.systemicmarking.SystemicObjectTracker.executeSystemically(SystemicObjectTracker.java:66)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doSerialApplyUpdate(SelectColumnLayer.java:311)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.lambda$createUpdateHandler$2(SelectColumnLayer.java:249)
	at io.deephaven.engine.table.impl.util.OperationInitializerJobScheduler.wrapRunnable(OperationInitializerJobScheduler.java:71)
	at io.deephaven.engine.table.impl.util.OperationInitializerJobScheduler.lambda$submit$0(OperationInitializerJobScheduler.java:42)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:214)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:225)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:213)
	at io.deephaven.engine.table.impl.OperationInitializationThreadPool$1.lambda$newThread$0(OperationInitializationThreadPool.java:50)
	... 4 more



	at org.jpy.PyLib.executeCode(Native Method)
	at org.jpy.PyObject.executeCode(PyObject.java:172)
	at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
	at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:229)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:51)
	at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:229)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$0(AbstractScriptSession.java:169)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:214)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:225)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:213)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:169)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:77)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:123)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$7(ConsoleServiceGrpcImpl.java:204)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:1244)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.server.runner.scheduler.SchedulerModule$ThreadFactory.lambda$newThread$0(SchedulerModule.java:100)
	at org.jpy.PyLib.callAndReturnObject(Native Method)
	at org.jpy.PyObject.call(PyObject.java:607)
	at io.deephaven.server.console.python.DebuggingInitializer.lambda$createInitializer$0(DebuggingInitializer.java:46)
	at java.base/java.lang.Thread.run(Thread.java:1583)
```

</details>

<details className="error">
<summary> Without the internal Deephaven stack traces and the repeated copy of the first traceback at the end of the [`DHError`](/core/pydoc/code/deephaven.dherror.html#deephaven.dherror.DHError) message, the error is much shorter: </summary>

```
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: java.lang.RuntimeException: Error in Python interpreter:
Traceback (most recent call last):
  File "/opt/deephaven/venv/lib/python3.12/site-packages/deephaven/table.py", line 1372, in update
    return Table(j_table=self.j_table.update(*formulas))
                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
RuntimeError: io.deephaven.engine.exceptions.TableInitializationException: Error while initializing Update([Y]): an exception occurred while performing the initial select or update
caused by java.lang.RuntimeException: Error in Python interpreter:
Traceback (most recent call last):
  File "<string>", line 7, in _wrapper
  File "<string>", line 3, in f
Exception: oops 1

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "<string>", line 4, in <module>
  File "/opt/deephaven/venv/lib/python3.12/site-packages/deephaven/table.py", line 1374, in update
    raise DHError(e, "table update operation failed.") from e
deephaven.dherror.DHError: table update operation failed. : java.lang.RuntimeException: Error in Python interpreter:
```

</details>

This is much easier to work with. Read it from the bottom up:

- The final traceback shows that line 4 of the script, `t1 = t.update("Y = f(X)")`, called [`update`](../reference/table-operations/select/update.md), and that the call raised a [`DHError`](/core/pydoc/code/deephaven.dherror.html#deephaven.dherror.DHError). [Table operations](../conceptual/table-operations-overview.md) in Deephaven's Python API raise a [`DHError`](/core/pydoc/code/deephaven.dherror.html#deephaven.dherror.DHError) when they fail.
- The `caused by` section above it shows the original error: `Exception: oops 1`, raised on line 3 of function `f`.

Java marks the error that caused the one above it with a `caused by` or `Caused by:` line, so the original error is in the last one.

If you catch the [`DHError`](/core/pydoc/code/deephaven.dherror.html#deephaven.dherror.DHError) in your own code, its [`compact_traceback`](/core/pydoc/code/deephaven.dherror.html#deephaven.dherror.DHError.compact_traceback) property gives a shorter version of this output.

The traceback contains two kinds of frames you didn't write:

- `_wrapper` is a function that Deephaven generates to call `f` from the query engine. It also reports `File "<string>"`, but it is not your code.
- `table.py` is the file in the `deephaven` package that defines [`update`](../reference/table-operations/select/update.md). Its path and line numbers depend on your Deephaven version and installation.

As in the [`g` example](#exceptions-in-your-own-functions), line 3 in `f` counts from the start of the command that defined `f`, not of the command that called [`update`](../reference/table-operations/select/update.md).

### Ticking tables

A [ticking table](../conceptual/table-types.md#standard-streaming-tables) is a table whose rows change while the query runs, such as a [time table](./time-table.md) that adds a row every second. Errors in [ticking tables](../conceptual/table-types.md#standard-streaming-tables) are harder to trace than errors in [static tables](../conceptual/table-types.md#static-tables).

When you apply a method such as [`update`](../reference/table-operations/select/update.md) to a [ticking table](../conceptual/table-types.md#standard-streaming-tables), Deephaven first computes values for every row in the table, as it does for a [static table](../conceptual/table-types.md#static-tables). After that, it computes values only for added rows and for modified rows whose input columns changed. An error can therefore appear later, while the query runs.

This example calls `f` from [Exceptions in your own functions](#exceptions-in-your-own-functions), which raises an exception when its input is 10. The query runs for about 10 seconds before it fails, when `X` reaches 10.

```python skip-test
from deephaven import time_table

t = time_table("PT1S").update("X = i")
t1 = t.update("Y = f(X)")
```

<details className="error">
<summary> Click to see the full stack trace. </summary>

```
aph-updateExecutor-9 | .AsyncClientErrorNotifier | Error in table update: java.lang.RuntimeException: Error in Python interpreter:
Traceback (most recent call last):
  File "<string>", line 7, in _wrapper
  File "<string>", line 3, in f
Exception: oops 1

	at org.jpy.PyLib.callAndReturnValue(Native Method)
	at org.jpy.PyObject.call(PyObject.java:636)
	at io.deephaven.engine.table.impl.select.python.FormulaKernelPythonChunkedFunction.applyFormulaChunk(FormulaKernelPythonChunkedFunction.java:106)
	at io.deephaven.engine.table.impl.select.FormulaKernelTypedBase$ToTypedMethod.visit(FormulaKernelTypedBase.java:147)
	at io.deephaven.chunk.LongChunk.walk(LongChunk.java:136)
	at io.deephaven.engine.table.impl.select.FormulaKernelTypedBase.applyFormulaChunk(FormulaKernelTypedBase.java:42)
	at io.deephaven.engine.table.impl.select.formula.FormulaKernelAdapter.fillChunkHelper(FormulaKernelAdapter.java:299)
	at io.deephaven.engine.table.impl.select.formula.FormulaKernelAdapter.fillChunk(FormulaKernelAdapter.java:229)
	at io.deephaven.engine.table.impl.select.Formula.getChunk(Formula.java:161)
	at io.deephaven.engine.table.impl.sources.ViewColumnSource.getChunk(ViewColumnSource.java:204)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doApplyUpdate(SelectColumnLayer.java:502)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.lambda$doSerialApplyUpdate$7(SelectColumnLayer.java:312)
	at io.deephaven.engine.util.systemicmarking.SystemicObjectTracker.executeSystemically(SystemicObjectTracker.java:66)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doSerialApplyUpdate(SelectColumnLayer.java:311)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.lambda$createUpdateHandler$2(SelectColumnLayer.java:249)
	at io.deephaven.engine.table.impl.util.UpdateGraphJobScheduler$1.run(UpdateGraphJobScheduler.java:49)
	at io.deephaven.engine.updategraph.impl.BaseUpdateGraph.runNotification(BaseUpdateGraph.java:756)
	at io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph$ConcurrentNotificationProcessor.processSatisfiedNotifications(PeriodicUpdateGraph.java:841)
	at io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph$NotificationProcessorThreadFactory.lambda$newThread$0(PeriodicUpdateGraph.java:1114)
	at org.jpy.PyLib.callAndReturnObject(Native Method)
	at org.jpy.PyObject.call(PyObject.java:607)
	at io.deephaven.server.console.python.DebuggingInitializer.lambda$createInitializer$0(DebuggingInitializer.java:46)
	at java.base/java.lang.Thread.run(Thread.java:1583)
```

</details>

Here the log header says `Error in table update` instead of `Error running script`: the error happened while Deephaven updated the table, not while the console ran your code. The thread differs too. `aph-updateExecutor-9` is the end of `PeriodicUpdateGraph-updateExecutor-9`, a thread of the [update graph](../conceptual/periodic-update-graph-configuration.md), not the thread that runs console commands.

Unfortunately, the error doesn't name the [query string](./query-string-overview.md) or the line of your script that created the table. The traceback still names the function that raised the error, `f`, so search your code for [query strings](./query-string-overview.md) that call `f`.

The [query string](./query-string-overview.md) is missing because Deephaven [vectorizes](../conceptual/deephaven-mental-model.md#calling-python-from-formulas) the call to `f`. It passes a whole chunk (block) of rows to the generated `_wrapper` function at once, and `_wrapper` calls `f` for each row. Errors from these calls don't include the [query string](./query-string-overview.md). Deephaven usually [vectorizes](../conceptual/deephaven-mental-model.md#calling-python-from-formulas) a formula that is only a direct call to a Python function, such as `Y = f(X)`.

Other formulas run row by row in compiled code, and their errors start with `In formula:` followed by the [query string](./query-string-overview.md). For example, an error in `Y = f(X) + 1` starts with `In formula: Y = f(X) + 1`.

When a [ticking table](../conceptual/table-types.md#standard-streaming-tables) fails while it is open in the Deephaven IDE, its panel is covered by a warning icon and a message that begins with `Unable to open table`. The message gives an ID. To find the full error, search the [Deephaven logs](./logs.md) for that ID. For the `f` example, the log entry looks like this:

```
aph-updateExecutor-5 | i.d.s.s.SessionService    | Internal Error '5028000f-4f91-4308-b8ac-ed49c573e829' java.lang.RuntimeException: Error in Python interpreter:
Traceback (most recent call last):
  File "<string>", line 7, in _wrapper
  File "<string>", line 3, in f
Exception: oops 1
```

Not every ticking-table error hides the [query string](./query-string-overview.md). In the next example, a [query string](./query-string-overview.md) on a [ticking table](../conceptual/table-types.md#standard-streaming-tables) calls Python's `math.sqrt`, which raises an error once its input is negative.

```python skip-test
from deephaven import time_table
import math

t = time_table("PT1S").update(["X = 10 - i", "Y = math.sqrt(X)"])
```

The table runs without error at first. After about 11 seconds, when `X` reaches -1, the table fails.

<details className="error">
<summary> Click to see the exception. </summary>

```
aph-updateExecutor-8 | .AsyncClientErrorNotifier | Error in table update: io.deephaven.engine.table.impl.select.FormulaEvaluationException: In formula: Y = math.sqrt(X)
	at io.deephaven.temp.c_8d80ce8b6bd0dd0670fc088a2948ef49af1fdbba1dd999babeca2a9cebb423bbv65_0.Formula.applyFormulaPerItem(Formula.java:161)
	at io.deephaven.temp.c_8d80ce8b6bd0dd0670fc088a2948ef49af1fdbba1dd999babeca2a9cebb423bbv65_0.Formula.lambda$fillChunkHelper$4(Formula.java:150)
	at io.deephaven.engine.rowset.RowSequence.lambda$forAllRowKeys$0(RowSequence.java:175)
	at io.deephaven.engine.rowset.impl.singlerange.SingleRangeMixin.forEachRowKey(SingleRangeMixin.java:17)
	at io.deephaven.engine.rowset.RowSequence.forAllRowKeys(RowSequence.java:174)
	at io.deephaven.temp.c_8d80ce8b6bd0dd0670fc088a2948ef49af1fdbba1dd999babeca2a9cebb423bbv65_0.Formula.fillChunkHelper(Formula.java:148)
	at io.deephaven.temp.c_8d80ce8b6bd0dd0670fc088a2948ef49af1fdbba1dd999babeca2a9cebb423bbv65_0.Formula.fillChunk(Formula.java:125)
	at io.deephaven.engine.table.impl.select.Formula.getChunk(Formula.java:161)
	at io.deephaven.engine.table.impl.sources.ViewColumnSource.getChunk(ViewColumnSource.java:204)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doApplyUpdate(SelectColumnLayer.java:502)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.lambda$doSerialApplyUpdate$7(SelectColumnLayer.java:312)
	at io.deephaven.engine.util.systemicmarking.SystemicObjectTracker.executeSystemically(SystemicObjectTracker.java:66)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doSerialApplyUpdate(SelectColumnLayer.java:311)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.lambda$createUpdateHandler$2(SelectColumnLayer.java:249)
	at io.deephaven.engine.table.impl.util.UpdateGraphJobScheduler$1.run(UpdateGraphJobScheduler.java:49)
	at io.deephaven.engine.updategraph.impl.BaseUpdateGraph.runNotification(BaseUpdateGraph.java:756)
	at io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph$ConcurrentNotificationProcessor.processSatisfiedNotifications(PeriodicUpdateGraph.java:841)
	at io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph$NotificationProcessorThreadFactory.lambda$newThread$0(PeriodicUpdateGraph.java:1114)
	at org.jpy.PyLib.callAndReturnObject(Native Method)
	at org.jpy.PyObject.call(PyObject.java:607)
	at io.deephaven.server.console.python.DebuggingInitializer.lambda$createInitializer$0(DebuggingInitializer.java:46)
	at java.base/java.lang.Thread.run(Thread.java:1583)
Caused by: java.lang.RuntimeException: Error in Python interpreter:
ValueError: math domain error

	at org.jpy.PyLib.callAndReturnObject(Native Method)
	at org.jpy.PyObject.callMethod(PyObject.java:585)
	at io.deephaven.engine.util.PyCallableWrapperJpyImpl.call(PyCallableWrapperJpyImpl.java:392)
	at io.deephaven.temp.c_8d80ce8b6bd0dd0670fc088a2948ef49af1fdbba1dd999babeca2a9cebb423bbv65_0.Formula.applyFormulaPerItem(Formula.java:159)
	... 21 more
```

</details>

Here, the `ValueError` under `Caused by:` shows that the query passed a negative value to `math.sqrt`.

Unlike the ticking `f` example above, this error names the [query string](./query-string-overview.md) that failed. Deephaven doesn't [vectorize](../conceptual/deephaven-mental-model.md#calling-python-from-formulas) a call through a module, such as `math.sqrt(X)`, so the formula runs row by row. The `In formula:` line shows the [query string](./query-string-overview.md) as written, `Y = math.sqrt(X)`, so you can match it to your code.

## Formula compilation errors

Now let's dig into an example with a more subtle problem. A [query string](./query-string-overview.md) can also fail before Deephaven computes any values. When it refers to a column or variable that doesn't exist, Deephaven can't compile the [query string's](./query-string-overview.md) [formula](./formulas.md). The engine throws a [`FormulaCompilationException`](/core/javadoc/io/deephaven/engine/table/impl/select/FormulaCompilationException.html), and the [table operation](../conceptual/table-operations-overview.md) raises it in Python as a [`DHError`](/core/pydoc/code/deephaven.dherror.html#deephaven.dherror.DHError).

In this example, the same [query string](./query-string-overview.md) appears in two functions, but it is valid in only one of them.

```python skip-test
from deephaven import empty_table


def f1():
    return empty_table(10).update("X = i").update("Y = X * 3")


def f2():
    return empty_table(10).update("Y = X * 3")


f1()
f2()
```

<details className="error">
<summary>This results in a long and complex error message: </summary>

```
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: java.lang.RuntimeException: Error in Python interpreter:
Traceback (most recent call last):
  File "/opt/deephaven/venv/lib/python3.12/site-packages/deephaven/table.py", line 1372, in update
    return Table(j_table=self.j_table.update(*formulas))
                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
RuntimeError: io.deephaven.engine.table.impl.select.FormulaCompilationException: Formula compilation error for: X * 3
	at io.deephaven.engine.table.impl.select.DhFormulaColumn.initDef(DhFormulaColumn.java:210)
	at io.deephaven.engine.table.impl.select.SwitchColumn.initDef(SwitchColumn.java:65)
	at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer.createContext(SelectAndViewAnalyzer.java:128)
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$38(QueryTable.java:1853)
	at io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder.withNugget(QueryPerformanceRecorder.java:395)
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$39(QueryTable.java:1835)
	at io.deephaven.engine.table.impl.QueryTable.memoizeResult(QueryTable.java:3357)
	at io.deephaven.engine.table.impl.QueryTable.selectOrUpdate(QueryTable.java:1834)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:1813)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:94)
	at io.deephaven.api.TableOperationsDefaults.update(TableOperationsDefaults.java:94)
	at org.jpy.PyLib.executeCode(Native Method)
	at org.jpy.PyObject.executeCode(PyObject.java:172)
	at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
	at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:229)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:51)
	at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:229)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$0(AbstractScriptSession.java:169)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:214)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:225)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:213)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:169)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:77)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:123)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$7(ConsoleServiceGrpcImpl.java:204)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:1244)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.server.runner.scheduler.SchedulerModule$ThreadFactory.lambda$newThread$0(SchedulerModule.java:100)
	at org.jpy.PyLib.callAndReturnObject(Native Method)
	at org.jpy.PyObject.call(PyObject.java:607)
	at io.deephaven.server.console.python.DebuggingInitializer.lambda$createInitializer$0(DebuggingInitializer.java:46)
	at java.base/java.lang.Thread.run(Thread.java:1583)
caused by io.deephaven.engine.table.impl.lang.QueryLanguageParser$QueryLanguageParseException: 

Having trouble with the following expression:
Full expression           : X * 3
Expression having trouble : 
Exception type            : io.deephaven.engine.table.impl.lang.QueryLanguageParser$ParserResolutionFailure
Exception message         : Cannot find variable or class X

	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.resolveName(QueryLanguageParser.java:1458)
	at java.base/java.util.HashMap.computeIfAbsent(HashMap.java:1228)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.visit(QueryLanguageParser.java:1412)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.visit(QueryLanguageParser.java:135)
	at com.github.javaparser.ast.expr.NameExpr.accept(NameExpr.java:80)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.getTypeWithCaching(QueryLanguageParser.java:1182)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.visit(QueryLanguageParser.java:1568)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.visit(QueryLanguageParser.java:135)
	at com.github.javaparser.ast.expr.BinaryExpr.accept(BinaryExpr.java:140)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.<init>(QueryLanguageParser.java:326)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.<init>(QueryLanguageParser.java:219)
	at io.deephaven.engine.table.impl.select.codegen.FormulaAnalyzer.parseFormula(FormulaAnalyzer.java:240)
	at io.deephaven.engine.table.impl.select.codegen.FormulaAnalyzer.parseFormula(FormulaAnalyzer.java:122)
	at io.deephaven.engine.table.impl.select.DhFormulaColumn.initDef(DhFormulaColumn.java:189)
	... 34 more


The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "<string>", line 13, in <module>
  File "<string>", line 9, in f2
  File "/opt/deephaven/venv/lib/python3.12/site-packages/deephaven/table.py", line 1374, in update
    raise DHError(e, "table update operation failed.") from e
deephaven.dherror.DHError: table update operation failed. : Cannot find variable or class X
Traceback (most recent call last):
  File "/opt/deephaven/venv/lib/python3.12/site-packages/deephaven/table.py", line 1372, in update
    return Table(j_table=self.j_table.update(*formulas))
                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
RuntimeError: io.deephaven.engine.table.impl.select.FormulaCompilationException: Formula compilation error for: X * 3
	at io.deephaven.engine.table.impl.select.DhFormulaColumn.initDef(DhFormulaColumn.java:210)
	at io.deephaven.engine.table.impl.select.SwitchColumn.initDef(SwitchColumn.java:65)
	at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer.createContext(SelectAndViewAnalyzer.java:128)
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$38(QueryTable.java:1853)
	at io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder.withNugget(QueryPerformanceRecorder.java:395)
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$39(QueryTable.java:1835)
	at io.deephaven.engine.table.impl.QueryTable.memoizeResult(QueryTable.java:3357)
	at io.deephaven.engine.table.impl.QueryTable.selectOrUpdate(QueryTable.java:1834)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:1813)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:94)
	at io.deephaven.api.TableOperationsDefaults.update(TableOperationsDefaults.java:94)
	at org.jpy.PyLib.executeCode(Native Method)
	at org.jpy.PyObject.executeCode(PyObject.java:172)
	at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
	at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:229)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:51)
	at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:229)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$0(AbstractScriptSession.java:169)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:214)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:225)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:213)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:169)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:77)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:123)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$7(ConsoleServiceGrpcImpl.java:204)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:1244)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.server.runner.scheduler.SchedulerModule$ThreadFactory.lambda$newThread$0(SchedulerModule.java:100)
	at org.jpy.PyLib.callAndReturnObject(Native Method)
	at org.jpy.PyObject.call(PyObject.java:607)
	at io.deephaven.server.console.python.DebuggingInitializer.lambda$createInitializer$0(DebuggingInitializer.java:46)
	at java.base/java.lang.Thread.run(Thread.java:1583)
caused by io.deephaven.engine.table.impl.lang.QueryLanguageParser$QueryLanguageParseException: 

Having trouble with the following expression:
Full expression           : X * 3
Expression having trouble : 
Exception type            : io.deephaven.engine.table.impl.lang.QueryLanguageParser$ParserResolutionFailure
Exception message         : Cannot find variable or class X

	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.resolveName(QueryLanguageParser.java:1458)
	at java.base/java.util.HashMap.computeIfAbsent(HashMap.java:1228)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.visit(QueryLanguageParser.java:1412)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.visit(QueryLanguageParser.java:135)
	at com.github.javaparser.ast.expr.NameExpr.accept(NameExpr.java:80)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.getTypeWithCaching(QueryLanguageParser.java:1182)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.visit(QueryLanguageParser.java:1568)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.visit(QueryLanguageParser.java:135)
	at com.github.javaparser.ast.expr.BinaryExpr.accept(BinaryExpr.java:140)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.<init>(QueryLanguageParser.java:326)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.<init>(QueryLanguageParser.java:219)
	at io.deephaven.engine.table.impl.select.codegen.FormulaAnalyzer.parseFormula(FormulaAnalyzer.java:240)
	at io.deephaven.engine.table.impl.select.codegen.FormulaAnalyzer.parseFormula(FormulaAnalyzer.java:122)
	at io.deephaven.engine.table.impl.select.DhFormulaColumn.initDef(DhFormulaColumn.java:189)
	... 34 more



	at org.jpy.PyLib.executeCode(Native Method)
	at org.jpy.PyObject.executeCode(PyObject.java:172)
	at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
	at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:229)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:51)
	at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:229)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$0(AbstractScriptSession.java:169)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:214)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:225)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:213)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:169)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:77)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:123)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$7(ConsoleServiceGrpcImpl.java:204)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:1244)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.server.runner.scheduler.SchedulerModule$ThreadFactory.lambda$newThread$0(SchedulerModule.java:100)
	at org.jpy.PyLib.callAndReturnObject(Native Method)
	at org.jpy.PyObject.call(PyObject.java:607)
	at io.deephaven.server.console.python.DebuggingInitializer.lambda$createInitializer$0(DebuggingInitializer.java:46)
	at java.base/java.lang.Thread.run(Thread.java:1583)
```

</details>

<details className="error">
<summary> Without the internal Deephaven stack traces and the repeated copy of the first traceback at the end of the [`DHError`](/core/pydoc/code/deephaven.dherror.html#deephaven.dherror.DHError) message, the error is much shorter: </summary>

```
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: java.lang.RuntimeException: Error in Python interpreter:
Traceback (most recent call last):
  File "/opt/deephaven/venv/lib/python3.12/site-packages/deephaven/table.py", line 1372, in update
    return Table(j_table=self.j_table.update(*formulas))
                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
RuntimeError: io.deephaven.engine.table.impl.select.FormulaCompilationException: Formula compilation error for: X * 3
caused by io.deephaven.engine.table.impl.lang.QueryLanguageParser$QueryLanguageParseException: 

Having trouble with the following expression:
Full expression           : X * 3
Expression having trouble : 
Exception type            : io.deephaven.engine.table.impl.lang.QueryLanguageParser$ParserResolutionFailure
Exception message         : Cannot find variable or class X

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "<string>", line 13, in <module>
  File "<string>", line 9, in f2
  File "/opt/deephaven/venv/lib/python3.12/site-packages/deephaven/table.py", line 1374, in update
    raise DHError(e, "table update operation failed.") from e
deephaven.dherror.DHError: table update operation failed. : Cannot find variable or class X
```

</details>

Three lines of the short output locate the problem:

- `Exception message` says that [`update`](../reference/table-operations/select/update.md) could not find the variable `X`.
- `Full expression` shows the expression that failed to compile, `X * 3`. That is the right side of the [query string](./query-string-overview.md) `Y = X * 3`, which appears twice in the code.
- The last traceback shows that the failure occurred on line 9 of the console command, in `f2`.

Now that you know the location of the problem, you can see that the table that `f2` creates has no `X` column, so its [query string](./query-string-overview.md) refers to a column that doesn't exist.

## Tips for reading errors

Start by finding the error:

- An error in code you run appears in the console.
- If a [ticking table](../conceptual/table-types.md#standard-streaming-tables) fails after it starts running, look for `Error in table update` in the [Deephaven logs](./logs.md): the IDE's [Log panel](./logs.md#log-panel), the [Docker container output](./logs.md#retrieve-docker-container-logs), or the terminal of a server you [started with Gradle](../getting-started/launch-build.md).
- If the table's panel shows an ID, search the [Deephaven logs](./logs.md) for that ID.

Then work through these steps to read the error:

1. Skip the lines that start with `at`. They are internal Java calls.
2. In the last Python traceback, find the line that gives the error type and message, such as `Exception: oops 1` or `deephaven.dherror.DHError: table update operation failed. : Cannot find variable or class X`.
3. Find the `File "<string>"` frames. These frames point to the code you ran in the console. Skip `_wrapper` and the frames in Deephaven's own files, such as `table.py`.
4. Count each frame's line number from the first line of the console command that defined that code, not from the first line of your whole script.
5. When a [table operation](../conceptual/table-operations-overview.md) fails, read the `caused by` and `Caused by:` sections. The last one in a chain holds the original error.
6. Match the `In formula:` or `Full expression` text to the [query string](./query-string-overview.md) in your code.

Once you know which query and line failed, see [Triage errors in queries](./triage-errors.md) for ways to isolate the cause.

## Related documentation

- [Deephaven's table types](../conceptual/table-types.md)
- [Table operations overview](../conceptual/table-operations-overview.md)
- [How Deephaven works: A mental model](../conceptual/deephaven-mental-model.md)
- [Periodic Update Graph](../conceptual/periodic-update-graph-configuration.md)
- [Create static tables](./new-and-empty-table.md#empty_table)
- [Create a time table](./time-table.md)
- [Triage errors in queries](./triage-errors.md)
- [Retrieve logs](./logs.md)
- [Query string overview](./query-string-overview.md)
- [Formulas in query strings](./formulas.md)
- [Filters in query strings](./filters.md)
- [Python functions in query strings](./python-functions.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`DHError` Pydoc](/core/pydoc/code/deephaven.dherror.html#deephaven.dherror.DHError)
