---
title: Decipher Python errors
---

When you develop Python queries, you will eventually write code that causes an error. This guide will teach you how to read Python errors so that you can quickly find and fix problems.

## The basics

Errors in Python can be one of two types: syntax errors and exceptions.

- Syntax errors occur when the Python interpreter encounters invalid syntax.
- Exceptions are unexpected events that occur during a program's execution.

As an example, let's try to execute some code that is not proper Python.

```python skip-test
x === 9
```

<details className="error">
<summary> This produces some scary-looking output: </summary>

```
r-Scheduler-Serial-1 | i.d.s.s.SessionState      | Internal Error '2725fb52-d58d-4705-824d-062d4b630e0a' java.lang.RuntimeException: Error in Python interpreter:
Type: <class 'SyntaxError'>
Value: invalid syntax (<string>, line 1)
Line: <not available>
Namespace: <not available>
File: <not available>
Traceback (most recent call last):

        at org.jpy.PyLib.executeCode(PyLib.java:-2)
        at org.jpy.PyObject.executeCode(PyObject.java:138)
        at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
        at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:183)
        at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:50)
        at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:182)
        at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$1(AbstractScriptSession.java:145)
        at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:129)
        at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:140)
        at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:128)
        at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:145)
        at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:87)
        at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:113)
        at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$8(ConsoleServiceGrpcImpl.java:155)
        at io.deephaven.server.session.SessionState$ExportBuilder.lambda$submit$2(SessionState.java:1349)
        at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:884)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
        at java.util.concurrent.FutureTask.run(FutureTask.java:264)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at io.deephaven.server.runner.DeephavenApiServerModule$ThreadFactory.lambda$newThread$0(DeephavenApiServerModule.java:164)
        at java.lang.Thread.run(Thread.java:829)
```

</details>

This output contains the clues you need to track down what is wrong with your program:

1. A unique identifier for finding more information about the error in the Deephaven logs.
2. A description of the error type and location.
3. A class stack trace indicating the function calls that resulted in the error.

In this case, additional information on the error may be found by searching the Deephaven logs for `2725fb52-d58d-4705-824d-062d4b630e0a`, but for this simple case, it is not necessary. We can see that a `SyntaxError` occurred on line 1, the only line of our program.

The stack trace shows 22 lines of internal Deephaven code. In most cases, the internal call stack details are noise and can be ignored. As you work through a few errors, you will begin to get a feeling for what parts of the stack trace are _your_ code and what parts are internal Deephaven code that can be ignored.

## Create your own exception

Now let's create our own exception. Here we create a function `f(x)` that will throw an exception when `x` is either 10 or 20.

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
r-Scheduler-Serial-1 | i.d.s.s.SessionState      | Internal Error '5ba7fbb9-49a2-46bf-8b14-f315cf251b62' java.lang.RuntimeException: Error in Python interpreter:
Type: <class 'Exception'>
Value: oops 1
Line: 3
Namespace: f
File: <string>
Traceback (most recent call last):
  File "<string>", line 10, in <module>
  File "<string>", line 3, in f

        at org.jpy.PyLib.executeCode(PyLib.java:-2)
        at org.jpy.PyObject.executeCode(PyObject.java:138)
        at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
        at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:183)
        at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:50)
        at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:182)
        at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$1(AbstractScriptSession.java:145)
        at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:129)
        at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:140)
        at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:128)
        at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:145)
        at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:87)
        at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:113)
        at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$8(ConsoleServiceGrpcImpl.java:155)
        at io.deephaven.server.session.SessionState$ExportBuilder.lambda$submit$2(SessionState.java:1349)
        at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:884)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
        at java.util.concurrent.FutureTask.run(FutureTask.java:264)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at io.deephaven.server.runner.DeephavenApiServerModule$ThreadFactory.lambda$newThread$0(DeephavenApiServerModule.java:164)
        at java.lang.Thread.run(Thread.java:829)
```

</details>

Here we see that the error is an `Exception` with the error message `oops 1`. Furthermore, we can also see that the exception was raised on line 3 of the function `f`:

```
Line: 3
Namespace: f
File: <string>
```

> [!NOTE]
> Because the code was specified on the Deephaven command line, the file is `<string>`. If the code is contained in a file, the filename containing the code will be shown.

In this case, the stack trace is helpful. Here we see that the error occurred when the program executed line 10 (`f(10)`) followed by line 3 (`raise Exception("oops 1")`). From this stack trace, you can see that calling `f(1)` did not result in an error. Only calling `f(10)` resulted in an error.

```
Traceback (most recent call last):
  File "<string>", line 10, in <module>
  File "<string>", line 3, in f
```

Here `File "<string>"` indicates that the code was input through the command line. Code from a file will show the file name instead of `<string>`.

> [!NOTE]
> `in <module>` indicates that the code is called from the command line or program main.

Now that you have the hang of this, let's make the example a little more difficult.

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
<summary> Here you can see that the exception in `f` was triggered by line 5 in `g` (`x += f(10)`): </summary>

```
r-Scheduler-Serial-1 | i.d.s.s.SessionState      | Internal Error '6bf622f4-e2ab-42f1-94c0-e98d3f9ba219' java.lang.RuntimeException: Error in Python interpreter:
Type: <class 'Exception'>
Value: oops 1
Line: 3
Namespace: f
File: <string>
Traceback (most recent call last):
  File "<string>", line 8, in <module>
  File "<string>", line 5, in g
  File "<string>", line 3, in f

        at org.jpy.PyLib.executeCode(PyLib.java:-2)
        at org.jpy.PyObject.executeCode(PyObject.java:138)
        at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
        at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:183)
        at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:50)
        at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:182)
        at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$1(AbstractScriptSession.java:145)
        at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:129)
        at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:140)
        at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:128)
        at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:145)
        at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:87)
        at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:113)
        at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$8(ConsoleServiceGrpcImpl.java:155)
        at io.deephaven.server.session.SessionState$ExportBuilder.lambda$submit$2(SessionState.java:1349)
        at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:884)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
        at java.util.concurrent.FutureTask.run(FutureTask.java:264)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at io.deephaven.server.runner.DeephavenApiServerModule$ThreadFactory.lambda$newThread$0(DeephavenApiServerModule.java:164)
        at java.lang.Thread.run(Thread.java:829)
```

</details>

## Exceptions in Deephaven tables

Deephaven allows Python functions to be applied to Deephaven tables. When this happens, it is possible for an error to occur.

Here an error occurs when adding a new column to a static Deephaven table. Because the table is static, all calculations occur when [`update`](../reference/table-operations/select/update.md) is called.

```python skip-test
from deephaven import empty_table

t = empty_table(20).update("X = i")
t1 = t.update("Y = f(X)")
```

<details className="error">
<summary> This error message is far more complex: </summary>

```
r-Scheduler-Serial-1 | i.d.s.s.SessionState      | Internal Error '4b24014d-7c65-4d7a-bbbb-d65c3cdd9676' java.lang.RuntimeException: Error in Python interpreter:
Type: <class 'deephaven.dherror.DHError'>
Value: table update operation failed. : RuntimeError: java.lang.RuntimeException: Error in Python interpreter:
Traceback (most recent call last):
  File "/opt/deephaven-venv/lib/python3.7/site-packages/deephaven/table.py", line 469, in update
    return Table(j_table=self.j_table.update(*formulas))
RuntimeError: java.lang.RuntimeException: Error in Python interpreter:
Type: <class 'Exception'>
Value: oops 1
Line: 3
Namespace: f
File: <string>
Traceback (most recent call last):
  File "/opt/deephaven-venv/lib/python3.7/site-packages/deephaven/table.py", line 134, in wrapper
  File "<string>", line 3, in f

	at org.jpy.PyLib.callAndReturnValue(Native Method)
	at org.jpy.PyObject.call(PyObject.java:474)
	at io.deephaven.engine.table.impl.select.python.FormulaKernelPythonChunkedFunction.applyFormulaChunk(FormulaKernelPythonChunkedFunction.java:106)
	at io.deephaven.engine.table.impl.select.FormulaKernelTypedBase$ToTypedMethod.visit(FormulaKernelTypedBase.java:147)
	at io.deephaven.chunk.LongChunk.walk(LongChunk.java:124)
	at io.deephaven.engine.table.impl.select.FormulaKernelTypedBase.applyFormulaChunk(FormulaKernelTypedBase.java:42)
	at io.deephaven.engine.table.impl.select.formula.FormulaKernelAdapter.fillChunkHelper(FormulaKernelAdapter.java:299)
	at io.deephaven.engine.table.impl.select.formula.FormulaKernelAdapter.fillChunk(FormulaKernelAdapter.java:229)
	at io.deephaven.engine.table.impl.sources.ViewColumnSource.fillChunk(ViewColumnSource.java:219)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doApplyUpdate(SelectColumnLayer.java:371)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.lambda$doSerialApplyUpdate$1(SelectColumnLayer.java:239)
	at io.deephaven.engine.util.systemicmarking.SystemicObjectTracker.executeSystemically(SystemicObjectTracker.java:56)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doSerialApplyUpdate(SelectColumnLayer.java:238)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer$1.lambda$onAllRequiredColumnsCompleted$1(SelectColumnLayer.java:195)
	at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer$ImmediateJobScheduler.submit(SelectAndViewAnalyzer.java:644)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer$1.onAllRequiredColumnsCompleted(SelectColumnLayer.java:193)
	at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer$SelectLayerCompletionHandler.onLayerCompleted(SelectAndViewAnalyzer.java:485)
	at io.deephaven.engine.table.impl.select.analyzers.BaseLayer.applyUpdate(BaseLayer.java:75)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.applyUpdate(SelectColumnLayer.java:133)
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$33(QueryTable.java:1286)
	at io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder.withNugget(QueryPerformanceRecorder.java:518)
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$34(QueryTable.java:1239)
	at io.deephaven.engine.table.impl.QueryTable.memoizeResult(QueryTable.java:3106)
	at io.deephaven.engine.table.impl.QueryTable.selectOrUpdate(QueryTable.java:1238)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:1218)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:89)
	at io.deephaven.api.TableOperationsDefaults.update(TableOperationsDefaults.java:105)
	at org.jpy.PyLib.executeCode(Native Method)
	at org.jpy.PyObject.executeCode(PyObject.java:138)
	at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
	at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:183)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:50)
	at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:182)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$1(AbstractScriptSession.java:145)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:129)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:140)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:128)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:145)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:87)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:113)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$8(ConsoleServiceGrpcImpl.java:155)
	at io.deephaven.server.session.SessionState$ExportBuilder.lambda$submit$2(SessionState.java:1349)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:884)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
	at io.deephaven.server.runner.DeephavenApiServerModule$ThreadFactory.lambda$newThread$0(DeephavenApiServerModule.java:164)
	at java.base/java.lang.Thread.run(Thread.java:829)


Line: 471
Namespace: update
File: /opt/deephaven-venv/lib/python3.7/site-packages/deephaven/table.py
Traceback (most recent call last):
  File "<string>", line 4, in <module>
  File "/opt/deephaven-venv/lib/python3.7/site-packages/deephaven/table.py", line 471, in update

        at org.jpy.PyLib.executeCode(PyLib.java:-2)
        at org.jpy.PyObject.executeCode(PyObject.java:138)
        at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
        at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:183)
        at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:50)
        at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:182)
        at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$1(AbstractScriptSession.java:145)
        at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:129)
        at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:140)
        at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:128)
        at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:145)
        at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:87)
        at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:113)
        at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$8(ConsoleServiceGrpcImpl.java:155)
        at io.deephaven.server.session.SessionState$ExportBuilder.lambda$submit$2(SessionState.java:1349)
        at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:884)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
        at java.util.concurrent.FutureTask.run(FutureTask.java:264)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at io.deephaven.server.runner.DeephavenApiServerModule$ThreadFactory.lambda$newThread$0(DeephavenApiServerModule.java:164)
        at java.lang.Thread.run(Thread.java:829)
```

</details>

<details className="error">
<summary> If the internal Deephaven stack trace is ignored, the error can be simplified to: </summary>

```
r-Scheduler-Serial-1 | i.d.s.s.SessionState      | Internal Error '4b24014d-7c65-4d7a-bbbb-d65c3cdd9676' java.lang.RuntimeException: Error in Python interpreter:
Type: <class 'deephaven.dherror.DHError'>
Value: table update operation failed. : RuntimeError: java.lang.RuntimeException: Error in Python interpreter:
Traceback (most recent call last):
  File "/opt/deephaven-venv/lib/python3.7/site-packages/deephaven/table.py", line 469, in update
    return Table(j_table=self.j_table.update(*formulas))
RuntimeError: java.lang.RuntimeException: Error in Python interpreter:
Type: <class 'Exception'>
Value: oops 1
Line: 3
Namespace: f
File: <string>
Traceback (most recent call last):
  File "/opt/deephaven-venv/lib/python3.7/site-packages/deephaven/table.py", line 134, in wrapper
  File "<string>", line 3, in f

Line: 471
Namespace: update
File: /opt/deephaven-venv/lib/python3.7/site-packages/deephaven/table.py
Traceback (most recent call last):
  File "<string>", line 4, in <module>
  File "/opt/deephaven-venv/lib/python3.7/site-packages/deephaven/table.py", line 471, in update
```

</details>

This is much easier to work with, and it is easy to see that the exception occurred on line 3 of function `f` when it was called called by [`update`](../reference/table-operations/select/update.md).

Dynamic tables are more complicated. When a method such as [`update`](../reference/table-operations/select/update.md) is applied to a dynamic table, values are computed for all of the rows present in the table. This is the same as the static case. After this initialization, values are computed on new or modified rows. This means that errors may happen at a later point, while the dynamic query is executing.

In this example, the query runs for 10 seconds before experiencing an error.

```python skip-test
from deephaven import time_table

t = time_table("PT1S").update("X = i")
t1 = t.update("Y = f(X)")
```

Here the stack trace shows the failure in the table update calculation. Unfortunately, some useful details on the query string and original stack trace have been lost.

<!-- TODO: this is bad for a UX -->

<details className="error">
<summary> Click to see the full stack trace. </summary>

```
EFAULT.refreshThread | .AsyncClientErrorNotifier | Error in table update: java.lang.RuntimeException: Error in Python interpreter:
Type: <class 'Exception'>
Value: oops 1
Line: 3
Namespace: f
File: <string>
Traceback (most recent call last):
  File "/opt/deephaven-venv/lib/python3.7/site-packages/deephaven/table.py", line 134, in wrapper
  File "<string>", line 3, in f

        at org.jpy.PyLib.callAndReturnValue(PyLib.java:-2)
        at org.jpy.PyObject.call(PyObject.java:474)
        at io.deephaven.engine.table.impl.select.python.FormulaKernelPythonChunkedFunction.applyFormulaChunk(FormulaKernelPythonChunkedFunction.java:106)
        at io.deephaven.engine.table.impl.select.FormulaKernelTypedBase$ToTypedMethod.visit(FormulaKernelTypedBase.java:147)
        at io.deephaven.chunk.LongChunk.walk(LongChunk.java:124)
        at io.deephaven.engine.table.impl.select.FormulaKernelTypedBase.applyFormulaChunk(FormulaKernelTypedBase.java:42)
        at io.deephaven.engine.table.impl.select.formula.FormulaKernelAdapter.fillChunkHelper(FormulaKernelAdapter.java:299)
        at io.deephaven.engine.table.impl.select.formula.FormulaKernelAdapter.fillChunk(FormulaKernelAdapter.java:229)
        at io.deephaven.engine.table.impl.select.Formula.getChunk(Formula.java:161)
        at io.deephaven.engine.table.impl.sources.ViewColumnSource.getChunk(ViewColumnSource.java:204)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doApplyUpdate(SelectColumnLayer.java:407)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.lambda$doSerialApplyUpdate$1(SelectColumnLayer.java:239)
        at io.deephaven.engine.util.systemicmarking.SystemicObjectTracker.executeSystemically(SystemicObjectTracker.java:56)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doSerialApplyUpdate(SelectColumnLayer.java:238)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer$1.lambda$onAllRequiredColumnsCompleted$1(SelectColumnLayer.java:195)
        at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer$ImmediateJobScheduler.submit(SelectAndViewAnalyzer.java:644)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer$1.onAllRequiredColumnsCompleted(SelectColumnLayer.java:193)
        at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer$SelectLayerCompletionHandler.onLayerCompleted(SelectAndViewAnalyzer.java:485)
        at io.deephaven.engine.table.impl.select.analyzers.BaseLayer.applyUpdate(BaseLayer.java:75)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.applyUpdate(SelectColumnLayer.java:133)
        at io.deephaven.engine.table.impl.SelectOrUpdateListener.onUpdate(SelectOrUpdateListener.java:90)
        at io.deephaven.engine.table.impl.InstrumentedTableUpdateListener$Notification.lambda$run$0(InstrumentedTableUpdateListener.java:37)
        at io.deephaven.engine.table.impl.InstrumentedTableListenerBase$NotificationBase.doRunInternal(InstrumentedTableListenerBase.java:294)
        at io.deephaven.engine.table.impl.InstrumentedTableListenerBase$NotificationBase.doRun(InstrumentedTableListenerBase.java:272)
        at io.deephaven.engine.table.impl.InstrumentedTableUpdateListener$Notification.run(InstrumentedTableUpdateListener.java:37)
        at io.deephaven.engine.updategraph.NotificationQueue$Notification.runInContext(NotificationQueue.java:60)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.runNotification(UpdateGraphProcessor.java:1298)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor$QueueNotificationProcessor.doWork(UpdateGraphProcessor.java:1459)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.flushNormalNotificationsAndCompleteCycle(UpdateGraphProcessor.java:1177)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.flushNotificationsAndCompleteCycle(UpdateGraphProcessor.java:1122)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.lambda$doRefresh$10(UpdateGraphProcessor.java:1736)
        at io.deephaven.util.locks.FunctionalLock.doLocked(FunctionalLock.java:32)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.doRefresh(UpdateGraphProcessor.java:1726)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.refreshAllTables(UpdateGraphProcessor.java:1713)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.refreshTablesAndFlushNotifications(UpdateGraphProcessor.java:1567)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor$1.run(UpdateGraphProcessor.java:276)
```

</details>

In addition to the error message on the command line and in the logs, failed dynamic tables also show an error message in the Deephaven IDE.

![An error message displays in the Deephaven IDE](../assets/python-error-failed-table.png)

## More complex example

Now let's dig into a more complex example with a more subtle problem. Here, one of the Deephaven query strings is not valid.

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
r-Scheduler-Serial-1 | i.d.s.s.SessionState      | Internal Error '2138b473-895f-4657-96d8-8882929c8acf' java.lang.RuntimeException: Error in Python interpreter:
Type: <class 'deephaven.dherror.DHError'>
Value: table update operation failed. : Cannot find variable or class X
Traceback (most recent call last):
  File "/opt/deephaven-venv/lib/python3.7/site-packages/deephaven/table.py", line 469, in update
    return Table(j_table=self.j_table.update(*formulas))
RuntimeError: io.deephaven.engine.table.impl.select.FormulaCompilationException: Formula compilation error for: X * 3
	at io.deephaven.engine.table.impl.select.DhFormulaColumn.initDef(DhFormulaColumn.java:206)
	at io.deephaven.engine.table.impl.select.SwitchColumn.initDef(SwitchColumn.java:66)
	at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer.create(SelectAndViewAnalyzer.java:90)
	at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer.create(SelectAndViewAnalyzer.java:61)
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$33(QueryTable.java:1254)
	at io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder.withNugget(QueryPerformanceRecorder.java:518)
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$34(QueryTable.java:1239)
	at io.deephaven.engine.table.impl.QueryTable.memoizeResult(QueryTable.java:3106)
	at io.deephaven.engine.table.impl.QueryTable.selectOrUpdate(QueryTable.java:1238)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:1218)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:89)
	at io.deephaven.api.TableOperationsDefaults.update(TableOperationsDefaults.java:105)
	at org.jpy.PyLib.executeCode(Native Method)
	at org.jpy.PyObject.executeCode(PyObject.java:138)
	at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
	at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:183)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:50)
	at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:182)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$1(AbstractScriptSession.java:145)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:129)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:140)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:128)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:145)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:87)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:113)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$8(ConsoleServiceGrpcImpl.java:155)
	at io.deephaven.server.session.SessionState$ExportBuilder.lambda$submit$2(SessionState.java:1349)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:884)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
	at io.deephaven.server.runner.DeephavenApiServerModule$ThreadFactory.lambda$newThread$0(DeephavenApiServerModule.java:164)
	at java.base/java.lang.Thread.run(Thread.java:829)
caused by io.deephaven.engine.table.impl.lang.QueryLanguageParser$QueryLanguageParseException:

Having trouble with the following expression:
Full expression           : X * 3
Expression having trouble : io.deephaven.engine.table.impl.lang.QueryLanguageParser$VisitArgs@766347b0
Exception message         : Cannot find variable or class X

	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.visit(QueryLanguageParser.java:1071)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.visit(QueryLanguageParser.java:134)
	at com.github.javaparser.ast.expr.NameExpr.accept(NameExpr.java:79)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.getTypeWithCaching(QueryLanguageParser.java:857)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.visit(QueryLanguageParser.java:1164)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.visit(QueryLanguageParser.java:134)
	at com.github.javaparser.ast.expr.BinaryExpr.accept(BinaryExpr.java:140)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.<init>(QueryLanguageParser.java:225)
	at io.deephaven.engine.table.impl.lang.QueryLanguageParser.<init>(QueryLanguageParser.java:178)
	at io.deephaven.engine.table.impl.select.codegen.FormulaAnalyzer.getCompiledFormula(FormulaAnalyzer.java:131)
	at io.deephaven.engine.table.impl.select.DhFormulaColumn.initDef(DhFormulaColumn.java:188)
	... 33 more


Line: 471
Namespace: update
File: /opt/deephaven-venv/lib/python3.7/site-packages/deephaven/table.py
Traceback (most recent call last):
  File "<string>", line 13, in <module>
  File "<string>", line 10, in f2
  File "/opt/deephaven-venv/lib/python3.7/site-packages/deephaven/table.py", line 471, in update

        at org.jpy.PyLib.executeCode(PyLib.java:-2)
        at org.jpy.PyObject.executeCode(PyObject.java:138)
        at io.deephaven.engine.util.PythonEvaluatorJpy.evalScript(PythonEvaluatorJpy.java:73)
        at io.deephaven.integrations.python.PythonDeephavenSession.lambda$evaluate$1(PythonDeephavenSession.java:183)
        at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:50)
        at io.deephaven.integrations.python.PythonDeephavenSession.evaluate(PythonDeephavenSession.java:182)
        at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$1(AbstractScriptSession.java:145)
        at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:129)
        at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:140)
        at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:128)
        at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:145)
        at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:87)
        at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:113)
        at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$8(ConsoleServiceGrpcImpl.java:155)
        at io.deephaven.server.session.SessionState$ExportBuilder.lambda$submit$2(SessionState.java:1349)
        at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:884)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
        at java.util.concurrent.FutureTask.run(FutureTask.java:264)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at io.deephaven.server.runner.DeephavenApiServerModule$ThreadFactory.lambda$newThread$0(DeephavenApiServerModule.java:164)
        at java.lang.Thread.run(Thread.java:829)
```

</details>

<details className="error">
<summary> But, removing the internal Deephaven stack traces yields a concise error: </summary>

```
r-Scheduler-Serial-1 | i.d.s.s.SessionState      | Internal Error '2138b473-895f-4657-96d8-8882929c8acf' java.lang.RuntimeException: Error in Python interpreter:
Type: <class 'deephaven.dherror.DHError'>
Value: table update operation failed. : Cannot find variable or class X
Traceback (most recent call last):
  File "/opt/deephaven-venv/lib/python3.7/site-packages/deephaven/table.py", line 469, in update
    return Table(j_table=self.j_table.update(*formulas))
RuntimeError: io.deephaven.engine.table.impl.select.FormulaCompilationException: Formula compilation error for: X * 3
caused by io.deephaven.engine.table.impl.lang.QueryLanguageParser$QueryLanguageParseException:

Having trouble with the following expression:
Full expression           : X * 3
Expression having trouble : io.deephaven.engine.table.impl.lang.QueryLanguageParser$VisitArgs@766347b0
Exception message         : Cannot find variable or class X


Line: 471
Namespace: update
File: /opt/deephaven-venv/lib/python3.7/site-packages/deephaven/table.py
Traceback (most recent call last):
  File "<string>", line 13, in <module>
  File "<string>", line 10, in f2
  File "/opt/deephaven-venv/lib/python3.7/site-packages/deephaven/table.py", line 471, in update
```

</details>

The error shows that [`update`](../reference/table-operations/select/update.md) could not find the variable `X` when trying to interpret `X * 3` in a query string. Looking at the original code, `X * 3` appears in two places. To figure out which one caused the problem, it is necessary to look at the stack trace. The stack trace indicates that the problem occurred in line 10 of the input, in `f2`. Now that you know the location of the problem, you can see that the table does not contain a column `X`, so the query string does not make sense.

## One more complex example

In this last example, a dynamic table calls Python's `math.sqrt` with a negative input.

```python skip-test
from deephaven import time_table
import math

t = time_table("PT1S").update(["X = 10 - i", "Y = math.sqrt(X)"])
```

<details className="error">
<summary> Click to see the exception. </summary>

```
EFAULT.refreshThread | .AsyncClientErrorNotifier | Error in table update: io.deephaven.engine.table.impl.select.FormulaEvaluationException: In formula: Y = (new io.deephaven.engine.util.PyCallableWrapper(math.getAttribute("sqrt"))).call(X)
        at io.deephaven.temp.c_fcea279bf739c80fdd860dca3582e4cb3c1d7d8f5443a3a1c467eccb8d2521a9v55_0.Formula.applyFormulaPerItem(Formula.java:153)
        at io.deephaven.temp.c_fcea279bf739c80fdd860dca3582e4cb3c1d7d8f5443a3a1c467eccb8d2521a9v55_0.Formula.lambda$fillChunkHelper$4(Formula.java:142)
        at io.deephaven.engine.rowset.RowSequence.lambda$forAllRowKeys$0(RowSequence.java:179)
        at io.deephaven.engine.rowset.impl.singlerange.SingleRangeMixin.forEachRowKey(SingleRangeMixin.java:17)
        at io.deephaven.engine.rowset.RowSequence.forAllRowKeys(RowSequence.java:178)
        at io.deephaven.temp.c_fcea279bf739c80fdd860dca3582e4cb3c1d7d8f5443a3a1c467eccb8d2521a9v55_0.Formula.fillChunkHelper(Formula.java:140)
        at io.deephaven.temp.c_fcea279bf739c80fdd860dca3582e4cb3c1d7d8f5443a3a1c467eccb8d2521a9v55_0.Formula.fillChunk(Formula.java:117)
        at io.deephaven.engine.table.impl.select.Formula.getChunk(Formula.java:161)
        at io.deephaven.engine.table.impl.sources.ViewColumnSource.getChunk(ViewColumnSource.java:204)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doApplyUpdate(SelectColumnLayer.java:407)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.lambda$doSerialApplyUpdate$1(SelectColumnLayer.java:239)
        at io.deephaven.engine.util.systemicmarking.SystemicObjectTracker.executeSystemically(SystemicObjectTracker.java:56)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doSerialApplyUpdate(SelectColumnLayer.java:238)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer$1.lambda$onAllRequiredColumnsCompleted$1(SelectColumnLayer.java:195)
        at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer$ImmediateJobScheduler.submit(SelectAndViewAnalyzer.java:644)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer$1.onAllRequiredColumnsCompleted(SelectColumnLayer.java:193)
        at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer$SelectLayerCompletionHandler.onLayerCompleted(SelectAndViewAnalyzer.java:485)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doSerialApplyUpdate(SelectColumnLayer.java:246)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer$1.lambda$onAllRequiredColumnsCompleted$1(SelectColumnLayer.java:195)
        at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer$ImmediateJobScheduler.submit(SelectAndViewAnalyzer.java:644)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer$1.onAllRequiredColumnsCompleted(SelectColumnLayer.java:193)
        at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer$SelectLayerCompletionHandler.onLayerCompleted(SelectAndViewAnalyzer.java:485)
        at io.deephaven.engine.table.impl.select.analyzers.BaseLayer.applyUpdate(BaseLayer.java:75)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.applyUpdate(SelectColumnLayer.java:133)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.applyUpdate(SelectColumnLayer.java:133)
        at io.deephaven.engine.table.impl.SelectOrUpdateListener.onUpdate(SelectOrUpdateListener.java:90)
        at io.deephaven.engine.table.impl.InstrumentedTableUpdateListener$Notification.lambda$run$0(InstrumentedTableUpdateListener.java:37)
        at io.deephaven.engine.table.impl.InstrumentedTableListenerBase$NotificationBase.doRunInternal(InstrumentedTableListenerBase.java:294)
        at io.deephaven.engine.table.impl.InstrumentedTableListenerBase$NotificationBase.doRun(InstrumentedTableListenerBase.java:272)
        at io.deephaven.engine.table.impl.InstrumentedTableUpdateListener$Notification.run(InstrumentedTableUpdateListener.java:37)
        at io.deephaven.engine.updategraph.NotificationQueue$Notification.runInContext(NotificationQueue.java:60)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.runNotification(UpdateGraphProcessor.java:1298)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor$QueueNotificationProcessor.doWork(UpdateGraphProcessor.java:1459)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.flushNormalNotificationsAndCompleteCycle(UpdateGraphProcessor.java:1177)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.flushNotificationsAndCompleteCycle(UpdateGraphProcessor.java:1122)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.lambda$doRefresh$10(UpdateGraphProcessor.java:1736)
        at io.deephaven.util.locks.FunctionalLock.doLocked(FunctionalLock.java:32)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.doRefresh(UpdateGraphProcessor.java:1726)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.refreshAllTables(UpdateGraphProcessor.java:1713)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.refreshTablesAndFlushNotifications(UpdateGraphProcessor.java:1567)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor$1.run(UpdateGraphProcessor.java:276)
caused by:
java.lang.RuntimeException: Error in Python interpreter:
Type: <class 'ValueError'>
Value: math domain error
Line: <not available>
Namespace: <not available>
File: <not available>
Traceback (most recent call last):

        at org.jpy.PyLib.callAndReturnObject(PyLib.java:-2)
        at org.jpy.PyObject.callMethod(PyObject.java:432)
        at io.deephaven.engine.util.PyCallableWrapper.call(PyCallableWrapper.java:222)
        at io.deephaven.temp.c_fcea279bf739c80fdd860dca3582e4cb3c1d7d8f5443a3a1c467eccb8d2521a9v55_0.Formula.applyFormulaPerItem(Formula.java:151)
        at io.deephaven.temp.c_fcea279bf739c80fdd860dca3582e4cb3c1d7d8f5443a3a1c467eccb8d2521a9v55_0.Formula.lambda$fillChunkHelper$4(Formula.java:142)
        at io.deephaven.engine.rowset.RowSequence.lambda$forAllRowKeys$0(RowSequence.java:179)
        at io.deephaven.engine.rowset.impl.singlerange.SingleRangeMixin.forEachRowKey(SingleRangeMixin.java:17)
        at io.deephaven.engine.rowset.RowSequence.forAllRowKeys(RowSequence.java:178)
        at io.deephaven.temp.c_fcea279bf739c80fdd860dca3582e4cb3c1d7d8f5443a3a1c467eccb8d2521a9v55_0.Formula.fillChunkHelper(Formula.java:140)
        at io.deephaven.temp.c_fcea279bf739c80fdd860dca3582e4cb3c1d7d8f5443a3a1c467eccb8d2521a9v55_0.Formula.fillChunk(Formula.java:117)
        at io.deephaven.engine.table.impl.select.Formula.getChunk(Formula.java:161)
        at io.deephaven.engine.table.impl.sources.ViewColumnSource.getChunk(ViewColumnSource.java:204)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doApplyUpdate(SelectColumnLayer.java:407)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.lambda$doSerialApplyUpdate$1(SelectColumnLayer.java:239)
        at io.deephaven.engine.util.systemicmarking.SystemicObjectTracker.executeSystemically(SystemicObjectTracker.java:56)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doSerialApplyUpdate(SelectColumnLayer.java:238)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer$1.lambda$onAllRequiredColumnsCompleted$1(SelectColumnLayer.java:195)
        at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer$ImmediateJobScheduler.submit(SelectAndViewAnalyzer.java:644)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer$1.onAllRequiredColumnsCompleted(SelectColumnLayer.java:193)
        at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer$SelectLayerCompletionHandler.onLayerCompleted(SelectAndViewAnalyzer.java:485)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doSerialApplyUpdate(SelectColumnLayer.java:246)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer$1.lambda$onAllRequiredColumnsCompleted$1(SelectColumnLayer.java:195)
        at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer$ImmediateJobScheduler.submit(SelectAndViewAnalyzer.java:644)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer$1.onAllRequiredColumnsCompleted(SelectColumnLayer.java:193)
        at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer$SelectLayerCompletionHandler.onLayerCompleted(SelectAndViewAnalyzer.java:485)
        at io.deephaven.engine.table.impl.select.analyzers.BaseLayer.applyUpdate(BaseLayer.java:75)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.applyUpdate(SelectColumnLayer.java:133)
        at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.applyUpdate(SelectColumnLayer.java:133)
        at io.deephaven.engine.table.impl.SelectOrUpdateListener.onUpdate(SelectOrUpdateListener.java:90)
        at io.deephaven.engine.table.impl.InstrumentedTableUpdateListener$Notification.lambda$run$0(InstrumentedTableUpdateListener.java:37)
        at io.deephaven.engine.table.impl.InstrumentedTableListenerBase$NotificationBase.doRunInternal(InstrumentedTableListenerBase.java:294)
        at io.deephaven.engine.table.impl.InstrumentedTableListenerBase$NotificationBase.doRun(InstrumentedTableListenerBase.java:272)
        at io.deephaven.engine.table.impl.InstrumentedTableUpdateListener$Notification.run(InstrumentedTableUpdateListener.java:37)
        at io.deephaven.engine.updategraph.NotificationQueue$Notification.runInContext(NotificationQueue.java:60)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.runNotification(UpdateGraphProcessor.java:1298)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor$QueueNotificationProcessor.doWork(UpdateGraphProcessor.java:1459)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.flushNormalNotificationsAndCompleteCycle(UpdateGraphProcessor.java:1177)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.flushNotificationsAndCompleteCycle(UpdateGraphProcessor.java:1122)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.lambda$doRefresh$10(UpdateGraphProcessor.java:1736)
        at io.deephaven.util.locks.FunctionalLock.doLocked(FunctionalLock.java:32)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.doRefresh(UpdateGraphProcessor.java:1726)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.refreshAllTables(UpdateGraphProcessor.java:1713)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor.refreshTablesAndFlushNotifications(UpdateGraphProcessor.java:1567)
        at io.deephaven.engine.updategraph.UpdateGraphProcessor$1.run(UpdateGraphProcessor.java:276)
```

</details>

Here, the `ValueError` indicates that `math.sqrt` was called with an improper value. The offending query string appears as `Y = (new io.deephaven.engine.util.PyCallableWrapper(math.getAttribute("sqrt"))).call(X)`, instead of the original and more concise `Y = math.sqrt(X)`.

<!-- TODO: this sucks -->

## Related documentation

- [Create an empty table](./new-and-empty-table.md#empty_table)
- [How to create a time table](./time-table.md)
- [How to triage errors](./triage-errors.md)
- [Filters in query strings](./filters.md)
- [Formulas in query strings](./formulas.md)
- [Operators in query strings](./operators.md)
