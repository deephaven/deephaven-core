---
title: Decipher Groovy errors
---

When you develop Groovy queries, you will eventually encounter errors. This guide teaches you how to read Groovy error output so you can find and fix problems quickly.

## The basics

Errors in Groovy fall into two categories:

- **Compilation errors** occur before execution, when the Groovy compiler finds invalid syntax.
- **Runtime exceptions** are unexpected events that occur while code is executing.

Unlike Python errors—which have a structured `Type:` / `Value:` / `Line:` format—Groovy errors appear as Java exception stack traces. When one exception causes another, they chain together using `Caused by:` lines. The root cause is always the last `Caused by:` entry in the chain.

As an example, let's execute a line of code with a syntax error:

```groovy should-fail
println("hello"
```

<details className="error">
<summary>This produces output like this:</summary>

```
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: io.deephaven.engine.util.GroovyExceptionWrapper: startup failed:
Script_6: 5: Unexpected input: ';\nprintln("hello"\n\n// this final true prevents Groovy from interpreting a trailing class definition as something to execute\n;' @ line 5, column 1.
   ;
   ^

1 error

	at org.codehaus.groovy.control.ErrorCollector.failIfErrors(ErrorCollector.java:292)
	at org.codehaus.groovy.control.ErrorCollector.addFatalError(ErrorCollector.java:148)
	at org.apache.groovy.parser.antlr4.AstBuilder.collectSyntaxError(AstBuilder.java:4260)
	at org.apache.groovy.parser.antlr4.AstBuilder.access$000(AstBuilder.java:157)
	at org.apache.groovy.parser.antlr4.AstBuilder$1.syntaxError(AstBuilder.java:4271)
	at groovyjarjarantlr4.v4.runtime.ProxyErrorListener.syntaxError(ProxyErrorListener.java:44)
	at groovyjarjarantlr4.v4.runtime.Parser.notifyErrorListeners(Parser.java:543)
	at groovyjarjarantlr4.v4.runtime.DefaultErrorStrategy.notifyErrorListeners(DefaultErrorStrategy.java:154)
	at org.apache.groovy.parser.antlr4.internal.DescriptiveErrorStrategy.reportNoViableAlternative(DescriptiveErrorStrategy.java:92)
	at groovyjarjarantlr4.v4.runtime.DefaultErrorStrategy.reportError(DefaultErrorStrategy.java:139)
	at org.apache.groovy.parser.antlr4.GroovyParser.compilationUnit(GroovyParser.java:367)
	at org.apache.groovy.parser.antlr4.AstBuilder.buildCST(AstBuilder.java:231)
	at org.apache.groovy.parser.antlr4.AstBuilder.buildCST(AstBuilder.java:209)
	at org.apache.groovy.parser.antlr4.AstBuilder.buildAST(AstBuilder.java:250)
	at org.apache.groovy.parser.antlr4.Antlr4ParserPlugin.buildAST(Antlr4ParserPlugin.java:58)
	at org.codehaus.groovy.control.SourceUnit.buildAST(SourceUnit.java:256)
	at java.base/java.util.Iterator.forEachRemaining(Iterator.java:133)
	at java.base/java.util.Spliterators$IteratorSpliterator.forEachRemaining(Spliterators.java:1939)
	at java.base/java.util.stream.ReferencePipeline$Head.forEach(ReferencePipeline.java:762)
	at org.codehaus.groovy.control.CompilationUnit.buildASTs(CompilationUnit.java:667)
	at org.codehaus.groovy.control.CompilationUnit.compile(CompilationUnit.java:633)
	at io.deephaven.engine.util.GroovyDeephavenSession.updateClassloader(GroovyDeephavenSession.java:665)
	at io.deephaven.engine.util.GroovyDeephavenSession.evaluate(GroovyDeephavenSession.java:348)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$0(AbstractScriptSession.java:169)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:214)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:225)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:213)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:169)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:77)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:123)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$7(ConsoleServiceGrpcImpl.java:204)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:1000)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.server.runner.scheduler.SchedulerModule$ThreadFactory.lambda$newThread$0(SchedulerModule.java:100)
	at java.base/java.lang.Thread.run(Thread.java:1583)
```

</details>

For compilation errors, Deephaven wraps the error in a `GroovyExceptionWrapper`. The key information is at the top of the output:

```
io.deephaven.engine.util.GroovyExceptionWrapper: startup failed:
Script_6: 5: Unexpected input: ... @ line 5, column 1.
```

The line and column numbers show where the syntax problem is.

> [!NOTE]
> Each time you execute code in the Deephaven console, it is compiled into a new class named `Script_N`, where `N` increments with each execution. You will see `Script_N.groovy` references in stack traces wherever your code appears. The line number within `Script_N.groovy` may be offset from what you see in the editor because Deephaven prepends some preamble (imports and setup) before compiling your script.

The remainder of the trace shows Groovy compiler and Deephaven session infrastructure frames. These can generally be ignored — the useful signal is in the error message itself.

## Create your own exception

Now let's look at a runtime exception. Here, a closure `f` throws when its argument is `10` or `20`:

```groovy should-fail
f = { int x ->
    if (x == 10) throw new Exception("oops 1")
    else if (x == 20) throw new Exception("oops 2")
    else return 1
}
f(1)
f(10)
```

<details className="error">
<summary>Click to see the full stack trace.</summary>

```
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: java.lang.RuntimeException: java.lang.Exception: oops 1
	at io.deephaven.engine.util.GroovyDeephavenSession.wrapAndRewriteStackTrace(GroovyDeephavenSession.java:367)
	at io.deephaven.engine.util.GroovyDeephavenSession.evaluate(GroovyDeephavenSession.java:356)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$0(AbstractScriptSession.java:169)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:214)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:225)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:213)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:169)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:77)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:123)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$7(ConsoleServiceGrpcImpl.java:204)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:1000)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.server.runner.scheduler.SchedulerModule$ThreadFactory.lambda$newThread$0(SchedulerModule.java:100)
	at java.base/java.lang.Thread.run(Thread.java:1583)
Caused by: java.lang.Exception: oops 1
	at java.base/jdk.internal.reflect.DirectConstructorHandleAccessor.newInstance(DirectConstructorHandleAccessor.java:62)
	at java.base/java.lang.reflect.Constructor.newInstanceWithCaller(Constructor.java:502)
	at java.base/java.lang.reflect.Constructor.newInstance(Constructor.java:486)
	at org.codehaus.groovy.reflection.CachedConstructor.invoke(CachedConstructor.java:72)
	at org.codehaus.groovy.runtime.callsite.ConstructorSite$ConstructorSiteNoUnwrapNoCoerce.callConstructor(ConstructorSite.java:105)
	at org.codehaus.groovy.runtime.callsite.CallSiteArray.defaultCallConstructor(CallSiteArray.java:59)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.callConstructor(AbstractCallSite.java:263)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.callConstructor(AbstractCallSite.java:277)
	at io.deephaven.dynamic.Script_6$_run_closure1.doCall(Script_6.groovy:3)
	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
	at org.codehaus.groovy.reflection.CachedMethod.invoke(CachedMethod.java:107)
	at groovy.lang.MetaMethod.doMethodInvoke(MetaMethod.java:323)
	at org.codehaus.groovy.runtime.metaclass.ClosureMetaClass.invokeMethod(ClosureMetaClass.java:274)
	at groovy.lang.MetaClassImpl.invokePropertyOrMissing(MetaClassImpl.java:1318)
	at groovy.lang.MetaClassImpl.invokeMethod(MetaClassImpl.java:1256)
	at org.codehaus.groovy.runtime.callsite.PogoMetaClassSite.callCurrent(PogoMetaClassSite.java:61)
	at org.codehaus.groovy.runtime.callsite.CallSiteArray.defaultCallCurrent(CallSiteArray.java:51)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.callCurrent(AbstractCallSite.java:171)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.callCurrent(AbstractCallSite.java:185)
	at io.deephaven.dynamic.Script_6.run(Script_6.groovy:8)
	at groovy.lang.GroovyShell.evaluate(GroovyShell.java:427)
	at groovy.lang.GroovyShell.evaluate(GroovyShell.java:461)
	at groovy.lang.GroovyShell.evaluate(GroovyShell.java:436)
	at io.deephaven.engine.util.GroovyDeephavenSession.lambda$evaluate$0(GroovyDeephavenSession.java:351)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:51)
	at io.deephaven.engine.util.GroovyDeephavenSession.evaluate(GroovyDeephavenSession.java:351)
	... 15 more
```

</details>

Deephaven wraps script-level exceptions in a `java.lang.RuntimeException` via `wrapAndRewriteStackTrace`. To find the actual error, follow the `Caused by:` chain.

> [!TIP]
> When reading a Groovy stack trace, start at the last `Caused by:` entry and work back toward the top. That is where the root cause is.

Within the `Caused by:` block, look for lines that reference `io.deephaven.dynamic.Script_N.groovy`. These point to your code:

```
at io.deephaven.dynamic.Script_6$_run_closure1.doCall(Script_6.groovy:3)   // inside f
...
at io.deephaven.dynamic.Script_6.run(Script_6.groovy:8)                    // f(10) call
```

> [!NOTE]
> Groovy closures appear in stack traces as `Script_N$_run_closureM`, where `M` is a number based on the closure's position in the script. This is how the Groovy compiler names anonymous closures internally. A named method would appear as `Script_N.methodName`. For more on closures, see [Groovy closures](./groovy-closures.md).

Now let's add another level of depth. The closure `g` calls `f` and accumulates the results:

```groovy should-fail
g = {
    def x = 0
    x += f(0)
    x += f(3)
    x += f(10)
    return x
}
g()
```

<details className="error">
<summary>Click to see the full stack trace.</summary>

```
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: java.lang.RuntimeException: java.lang.Exception: oops 1
	at io.deephaven.engine.util.GroovyDeephavenSession.wrapAndRewriteStackTrace(GroovyDeephavenSession.java:367)
	at io.deephaven.engine.util.GroovyDeephavenSession.evaluate(GroovyDeephavenSession.java:356)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$0(AbstractScriptSession.java:169)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:214)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:225)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:213)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:169)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:77)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:123)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$7(ConsoleServiceGrpcImpl.java:204)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:1000)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.server.runner.scheduler.SchedulerModule$ThreadFactory.lambda$newThread$0(SchedulerModule.java:100)
	at java.base/java.lang.Thread.run(Thread.java:1583)
Caused by: java.lang.Exception: oops 1
	at java.base/jdk.internal.reflect.DirectConstructorHandleAccessor.newInstance(DirectConstructorHandleAccessor.java:62)
	at java.base/java.lang.reflect.Constructor.newInstanceWithCaller(Constructor.java:502)
	at java.base/java.lang.reflect.Constructor.newInstance(Constructor.java:486)
	at org.codehaus.groovy.reflection.CachedConstructor.invoke(CachedConstructor.java:72)
	at org.codehaus.groovy.runtime.callsite.ConstructorSite$ConstructorSiteNoUnwrapNoCoerce.callConstructor(ConstructorSite.java:105)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.callConstructor(AbstractCallSite.java:277)
	at io.deephaven.dynamic.Script_6$_run_closure1.doCall(Script_6.groovy:3)
	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
	at org.codehaus.groovy.reflection.CachedMethod.invoke(CachedMethod.java:107)
	at groovy.lang.MetaMethod.doMethodInvoke(MetaMethod.java:323)
	at org.codehaus.groovy.runtime.metaclass.ClosureMetaClass.invokeMethod(ClosureMetaClass.java:274)
	at groovy.lang.MetaClassImpl.invokePropertyOrMissing(MetaClassImpl.java:1318)
	at groovy.lang.MetaClassImpl.invokeMethod(MetaClassImpl.java:1256)
	at groovy.lang.MetaClassImpl.invokeMethod(MetaClassImpl.java:1030)
	at groovy.lang.MetaClassImpl.invokeMethod(MetaClassImpl.java:814)
	at groovy.lang.GroovyObject.invokeMethod(GroovyObject.java:39)
	at groovy.lang.Script.invokeMethod(Script.java:96)
	at org.codehaus.groovy.runtime.metaclass.ClosureMetaClass.invokeOnDelegationObjects(ClosureMetaClass.java:408)
	at org.codehaus.groovy.runtime.metaclass.ClosureMetaClass.invokeMethod(ClosureMetaClass.java:348)
	at org.codehaus.groovy.runtime.callsite.PogoMetaClassSite.callCurrent(PogoMetaClassSite.java:61)
	at org.codehaus.groovy.runtime.callsite.CallSiteArray.defaultCallCurrent(CallSiteArray.java:51)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.callCurrent(AbstractCallSite.java:171)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.callCurrent(AbstractCallSite.java:185)
	at io.deephaven.dynamic.Script_7$_run_closure1.doCall(Script_7.groovy:6)
	at io.deephaven.dynamic.Script_7$_run_closure1.doCall(Script_7.groovy)
	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
	at org.codehaus.groovy.reflection.CachedMethod.invoke(CachedMethod.java:107)
	at groovy.lang.MetaMethod.doMethodInvoke(MetaMethod.java:323)
	at org.codehaus.groovy.runtime.metaclass.ClosureMetaClass.invokeMethod(ClosureMetaClass.java:274)
	at groovy.lang.MetaClassImpl.invokePropertyOrMissing(MetaClassImpl.java:1318)
	at groovy.lang.MetaClassImpl.invokeMethod(MetaClassImpl.java:1256)
	at org.codehaus.groovy.runtime.callsite.PogoMetaClassSite.callCurrent(PogoMetaClassSite.java:61)
	at org.codehaus.groovy.runtime.callsite.CallSiteArray.defaultCallCurrent(CallSiteArray.java:51)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.callCurrent(AbstractCallSite.java:171)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.callCurrent(AbstractCallSite.java:176)
	at io.deephaven.dynamic.Script_7.run(Script_7.groovy:9)
	at groovy.lang.GroovyShell.evaluate(GroovyShell.java:427)
	at groovy.lang.GroovyShell.evaluate(GroovyShell.java:461)
	at groovy.lang.GroovyShell.evaluate(GroovyShell.java:436)
	at io.deephaven.engine.util.GroovyDeephavenSession.lambda$evaluate$0(GroovyDeephavenSession.java:351)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:51)
	at io.deephaven.engine.util.GroovyDeephavenSession.evaluate(GroovyDeephavenSession.java:351)
	... 15 more
```

</details>

Because `f` and `g` were defined in separate console executions, they live in different compiled classes: `Script_6` and `Script_7`. The trace crosses those boundaries, showing the full call chain from the root cause upward:

```
at io.deephaven.dynamic.Script_6$_run_closure1.doCall(Script_6.groovy:3)   // inside f
...
at io.deephaven.dynamic.Script_7$_run_closure1.doCall(Script_7.groovy:6)   // x += f(10) in g
...
at io.deephaven.dynamic.Script_7.run(Script_7.groovy:9)                    // g() call
```

## Exceptions in Deephaven tables

Closures can be used inside table formulas. When they throw, the error becomes more complex. In this example, `f` is applied as a column formula on a static table:

```groovy should-fail
t = emptyTable(20).update("X = i")
t1 = t.update("Y = f(X)")
```

<details className="error">
<summary>Click to see the full stack trace.</summary>

```
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: io.deephaven.engine.exceptions.TableInitializationException: Error while initializing Update([Y]): an exception occurred while performing the initial select or update
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$38(QueryTable.java:1765)
	at io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder.withNugget(QueryPerformanceRecorder.java:390)
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$39(QueryTable.java:1702)
	at io.deephaven.engine.table.impl.QueryTable.memoizeResult(QueryTable.java:3216)
	at io.deephaven.engine.table.impl.QueryTable.selectOrUpdate(QueryTable.java:1701)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:1680)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:98)
	at io.deephaven.api.TableOperationsDefaults.update(TableOperationsDefaults.java:94)
	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
	at org.codehaus.groovy.runtime.callsite.PlainObjectMetaMethodSite.doInvoke(PlainObjectMetaMethodSite.java:48)
	at org.codehaus.groovy.runtime.callsite.PojoMetaMethodSite$PojoCachedMethodSite.invoke(PojoMetaMethodSite.java:186)
	at org.codehaus.groovy.runtime.callsite.PojoMetaMethodSite.call(PojoMetaMethodSite.java:51)
	at org.codehaus.groovy.runtime.callsite.CallSiteArray.defaultCall(CallSiteArray.java:47)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.call(AbstractCallSite.java:125)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.call(AbstractCallSite.java:139)
	at io.deephaven.dynamic.Script_8.run(Script_8.groovy:3)
	at groovy.lang.GroovyShell.evaluate(GroovyShell.java:427)
	at groovy.lang.GroovyShell.evaluate(GroovyShell.java:461)
	at groovy.lang.GroovyShell.evaluate(GroovyShell.java:436)
	at io.deephaven.engine.util.GroovyDeephavenSession.lambda$evaluate$0(GroovyDeephavenSession.java:351)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:51)
	at io.deephaven.engine.util.GroovyDeephavenSession.evaluate(GroovyDeephavenSession.java:351)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$0(AbstractScriptSession.java:169)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:214)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:225)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:213)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:169)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:77)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:123)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$7(ConsoleServiceGrpcImpl.java:204)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:1000)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.server.runner.scheduler.SchedulerModule$ThreadFactory.lambda$newThread$0(SchedulerModule.java:100)
	at java.base/java.lang.Thread.run(Thread.java:1583)
Caused by: io.deephaven.engine.table.impl.select.FormulaEvaluationException: In formula: Y = f(X)
	at io.deephaven.temp.c_791c810c3999324979ade86138ad780e97d21ce6802f7594b358fb980ccc0ba9v65_0.Formula.applyFormulaPerItem(Formula.java:160)
	at io.deephaven.temp.c_791c810c3999324979ade86138ad780e97d21ce6802f7594b358fb980ccc0ba9v65_0.Formula.lambda$fillChunkHelper$4(Formula.java:149)
	at io.deephaven.engine.rowset.RowSequence.lambda$forAllRowKeys$0(RowSequence.java:175)
	at io.deephaven.engine.rowset.impl.singlerange.SingleRangeMixin.forEachRowKey(SingleRangeMixin.java:17)
	at io.deephaven.engine.rowset.RowSequence.forAllRowKeys(RowSequence.java:174)
	at io.deephaven.temp.c_791c810c3999324979ade86138ad780e97d21ce6802f7594b358fb980ccc0ba9v65_0.Formula.fillChunkHelper(Formula.java:147)
	at io.deephaven.temp.c_791c810c3999324979ade86138ad780e97d21ce6802f7594b358fb980ccc0ba9v65_0.Formula.fillChunk(Formula.java:124)
	at io.deephaven.engine.table.impl.sources.ViewColumnSource.fillChunk(ViewColumnSource.java:219)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doApplyUpdate(SelectColumnLayer.java:466)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.lambda$doSerialApplyUpdate$7(SelectColumnLayer.java:312)
	at io.deephaven.engine.util.systemicmarking.SystemicObjectTracker.executeSystemically(SystemicObjectTracker.java:66)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doSerialApplyUpdate(SelectColumnLayer.java:311)
	at io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.lambda$createUpdateHandler$2(SelectColumnLayer.java:249)
	at io.deephaven.engine.table.impl.util.OperationInitializerJobScheduler.lambda$submit$0(OperationInitializerJobScheduler.java:40)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:214)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:225)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:213)
	at io.deephaven.engine.table.impl.OperationInitializationThreadPool$1.lambda$newThread$0(OperationInitializationThreadPool.java:50)
	... 1 more
Caused by: java.lang.Exception: oops 1
	at java.base/jdk.internal.reflect.DirectConstructorHandleAccessor.newInstance(DirectConstructorHandleAccessor.java:62)
	at java.base/java.lang.reflect.Constructor.newInstanceWithCaller(Constructor.java:502)
	at java.base/java.lang.reflect.Constructor.newInstance(Constructor.java:486)
	at org.codehaus.groovy.reflection.CachedConstructor.invoke(CachedConstructor.java:72)
	at org.codehaus.groovy.runtime.callsite.ConstructorSite$ConstructorSiteNoUnwrapNoCoerce.callConstructor(ConstructorSite.java:105)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.callConstructor(AbstractCallSite.java:277)
	at io.deephaven.dynamic.Script_6$_run_closure1.doCall(Script_6.groovy:3)
	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
	at org.codehaus.groovy.reflection.CachedMethod.invoke(CachedMethod.java:107)
	at groovy.lang.MetaMethod.doMethodInvoke(MetaMethod.java:323)
	at org.codehaus.groovy.runtime.metaclass.ClosureMetaClass.invokeMethod(ClosureMetaClass.java:274)
	at groovy.lang.MetaClassImpl.invokeMethod(MetaClassImpl.java:1030)
	at groovy.lang.Closure.call(Closure.java:427)
	at groovy.lang.Closure.call(Closure.java:416)
	at io.deephaven.temp.c_791c810c3999324979ade86138ad780e97d21ce6802f7594b358fb980ccc0ba9v65_0.Formula.applyFormulaPerItem(Formula.java:158)
	... 22 more
```

</details>

Working through the `Caused by:` chain from the bottom:

1. `java.lang.Exception: oops 1` — the root cause, thrown inside the closure `f`.
2. `FormulaEvaluationException: In formula: Y = f(X)` — the query engine detected a failure while evaluating that formula.
3. `TableInitializationException: Error while initializing Update([Y])` — the table failed to initialize because of the above.

The line `Script_8.groovy:3` tells you which line in your code triggered the failing `update` call.

Ticking tables behave differently. When a formula is applied to a ticking table, it is evaluated for existing rows at creation time, then again for every new row as it arrives. Errors may appear well after the table is open and apparently working.

```groovy should-fail
t2 = timeTable("PT1S").update("X = i")
t3 = t2.update("Y = f(X)")
```

<details className="error">
<summary>Click to see the full stack trace.</summary>

```
aph-updateExecutor-5 | .AsyncClientErrorNotifier | Error in table update: io.deephaven.engine.table.impl.select.FormulaEvaluationException: In formula: Y = f(X)
	at io.deephaven.temp.c_791c810c3999324979ade86138ad780e97d21ce6802f7594b358fb980ccc0ba9v65_0.Formula.applyFormulaPerItem(Formula.java:160)
	at io.deephaven.temp.c_791c810c3999324979ade86138ad780e97d21ce6802f7594b358fb980ccc0ba9v65_0.Formula.lambda$fillChunkHelper$4(Formula.java:149)
	at io.deephaven.engine.rowset.RowSequence.lambda$forAllRowKeys$0(RowSequence.java:175)
	at io.deephaven.engine.rowset.impl.singlerange.SingleRangeMixin.forEachRowKey(SingleRangeMixin.java:17)
	at io.deephaven.engine.rowset.RowSequence.forAllRowKeys(RowSequence.java:174)
	at io.deephaven.temp.c_791c810c3999324979ade86138ad780e97d21ce6802f7594b358fb980ccc0ba9v65_0.Formula.fillChunkHelper(Formula.java:147)
	at io.deephaven.temp.c_791c810c3999324979ade86138ad780e97d21ce6802f7594b358fb980ccc0ba9v65_0.Formula.fillChunk(Formula.java:124)
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
	at java.base/java.lang.Thread.run(Thread.java:1583)
Caused by: java.lang.Exception: oops 1
	at java.base/jdk.internal.reflect.DirectConstructorHandleAccessor.newInstance(DirectConstructorHandleAccessor.java:62)
	at java.base/java.lang.reflect.Constructor.newInstanceWithCaller(Constructor.java:502)
	at java.base/java.lang.reflect.Constructor.newInstance(Constructor.java:486)
	at org.codehaus.groovy.reflection.CachedConstructor.invoke(CachedConstructor.java:72)
	at org.codehaus.groovy.runtime.callsite.ConstructorSite$ConstructorSiteNoUnwrapNoCoerce.callConstructor(ConstructorSite.java:105)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.callConstructor(AbstractCallSite.java:277)
	at io.deephaven.dynamic.Script_6$_run_closure1.doCall(Script_6.groovy:3)
	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
	at org.codehaus.groovy.reflection.CachedMethod.invoke(CachedMethod.java:107)
	at groovy.lang.MetaMethod.doMethodInvoke(MetaMethod.java:323)
	at org.codehaus.groovy.runtime.metaclass.ClosureMetaClass.invokeMethod(ClosureMetaClass.java:274)
	at groovy.lang.MetaClassImpl.invokeMethod(MetaClassImpl.java:1030)
	at groovy.lang.Closure.call(Closure.java:427)
	at groovy.lang.Closure.call(Closure.java:416)
	at io.deephaven.temp.c_791c810c3999324979ade86138ad780e97d21ce6802f7594b358fb980ccc0ba9v65_0.Formula.applyFormulaPerItem(Formula.java:158)
	... 18 more
```

</details>

Several things differ from the static table error:

- **Thread prefix**: `aph-updateExecutor-N` instead of `r-Scheduler-Serial-1`. The error occurred on a background update thread, not the session thread.
- **Log source**: `.AsyncClientErrorNotifier | Error in table update:` instead of `.c.ConsoleServiceGrpcImpl | Error running script`.
- **No `TableInitializationException`**: The trace starts directly with `FormulaEvaluationException`. The table was created successfully; it only failed when a new row arrived.
- **No script line**: Because the error occurred asynchronously, there is no reference back to the line that created the table.

In addition to the server log, the Deephaven IDE marks the failed ticking table with an error indicator in the UI.

## More complex example

Now let's look at a formula that references a column that doesn't exist. `f1` uses column `X` correctly; `f2` does not:

```groovy should-fail
f1 = { return emptyTable(10).update("X = i").update("Y = X * 3") }
f2 = { return emptyTable(10).update("Y = X * 3") }
f1()
f2()
```

<details className="error">
<summary>Click to see the full stack trace.</summary>

```
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: io.deephaven.engine.table.impl.select.FormulaCompilationException: Formula compilation error for: X * 3
	at io.deephaven.engine.table.impl.select.DhFormulaColumn.initDef(DhFormulaColumn.java:210)
	at io.deephaven.engine.table.impl.select.SwitchColumn.initDef(SwitchColumn.java:65)
	at io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer.createContext(SelectAndViewAnalyzer.java:128)
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$38(QueryTable.java:1720)
	at io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder.withNugget(QueryPerformanceRecorder.java:390)
	at io.deephaven.engine.table.impl.QueryTable.lambda$selectOrUpdate$39(QueryTable.java:1702)
	at io.deephaven.engine.table.impl.QueryTable.memoizeResult(QueryTable.java:3216)
	at io.deephaven.engine.table.impl.QueryTable.selectOrUpdate(QueryTable.java:1701)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:1680)
	at io.deephaven.engine.table.impl.QueryTable.update(QueryTable.java:98)
	at io.deephaven.api.TableOperationsDefaults.update(TableOperationsDefaults.java:94)
	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
	at org.codehaus.groovy.runtime.callsite.PlainObjectMetaMethodSite.doInvoke(PlainObjectMetaMethodSite.java:48)
	at org.codehaus.groovy.runtime.callsite.PojoMetaMethodSite$PojoCachedMethodSite.invoke(PojoMetaMethodSite.java:186)
	at org.codehaus.groovy.runtime.callsite.PojoMetaMethodSite.call(PojoMetaMethodSite.java:51)
	at org.codehaus.groovy.runtime.callsite.CallSiteArray.defaultCall(CallSiteArray.java:47)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.call(AbstractCallSite.java:125)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.call(AbstractCallSite.java:139)
	at io.deephaven.dynamic.Script_11$_run_closure2.doCall(Script_11.groovy:3)
	at io.deephaven.dynamic.Script_11$_run_closure2.doCall(Script_11.groovy)
	at java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:103)
	at java.base/java.lang.reflect.Method.invoke(Method.java:580)
	at org.codehaus.groovy.reflection.CachedMethod.invoke(CachedMethod.java:107)
	at groovy.lang.MetaMethod.doMethodInvoke(MetaMethod.java:323)
	at org.codehaus.groovy.runtime.metaclass.ClosureMetaClass.invokeMethod(ClosureMetaClass.java:274)
	at groovy.lang.MetaClassImpl.invokePropertyOrMissing(MetaClassImpl.java:1318)
	at groovy.lang.MetaClassImpl.invokeMethod(MetaClassImpl.java:1256)
	at org.codehaus.groovy.runtime.callsite.PogoMetaClassSite.callCurrent(PogoMetaClassSite.java:61)
	at org.codehaus.groovy.runtime.callsite.CallSiteArray.defaultCallCurrent(CallSiteArray.java:51)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.callCurrent(AbstractCallSite.java:171)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.callCurrent(AbstractCallSite.java:176)
	at io.deephaven.dynamic.Script_11.run(Script_11.groovy:5)
	at groovy.lang.GroovyShell.evaluate(GroovyShell.java:427)
	at groovy.lang.GroovyShell.evaluate(GroovyShell.java:461)
	at groovy.lang.GroovyShell.evaluate(GroovyShell.java:436)
	at io.deephaven.engine.util.GroovyDeephavenSession.lambda$evaluate$0(GroovyDeephavenSession.java:351)
	at io.deephaven.util.locks.FunctionalLock.doLockedInterruptibly(FunctionalLock.java:51)
	at io.deephaven.engine.util.GroovyDeephavenSession.evaluate(GroovyDeephavenSession.java:351)
	at io.deephaven.engine.util.AbstractScriptSession.lambda$evaluateScript$0(AbstractScriptSession.java:169)
	at io.deephaven.engine.context.ExecutionContext.lambda$apply$0(ExecutionContext.java:214)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:225)
	at io.deephaven.engine.context.ExecutionContext.apply(ExecutionContext.java:213)
	at io.deephaven.engine.util.AbstractScriptSession.evaluateScript(AbstractScriptSession.java:169)
	at io.deephaven.engine.util.DelegatingScriptSession.evaluateScript(DelegatingScriptSession.java:77)
	at io.deephaven.engine.util.ScriptSession.evaluateScript(ScriptSession.java:123)
	at io.deephaven.server.console.ConsoleServiceGrpcImpl.lambda$executeCommand$7(ConsoleServiceGrpcImpl.java:204)
	at io.deephaven.server.session.SessionState$ExportObject.doExport(SessionState.java:1000)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at io.deephaven.server.runner.scheduler.SchedulerModule$ThreadFactory.lambda$newThread$0(SchedulerModule.java:100)
	at java.base/java.lang.Thread.run(Thread.java:1583)
Caused by: io.deephaven.engine.table.impl.lang.QueryLanguageParser$QueryLanguageParseException:

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
	... 53 more
```

</details>

The `Caused by:` chain:

1. `QueryLanguageParser$QueryLanguageParseException` — the Deephaven query language parser failed. The message is specific: it shows the full expression (`X * 3`) and the problem (`Cannot find variable or class X`).
2. `FormulaCompilationException: Formula compilation error for: X * 3` — the formula could not be compiled.

The script trace points to `Script_11$_run_closure2` (inside `f2`) and `Script_11.groovy:5` (the `f2()` call). The `_run_closure2` suffix indicates this is the second closure defined in the script (`f2`), confirming which function caused the problem.

This is a common mistake: referencing a column name in a formula that does not exist in the table at the point the formula is evaluated. Here, `f2` tries to use `X` in a table that has no `X` column.

## One more complex example

In this last example, a ticking table calls `Math.floorDiv`, which throws when its divisor reaches zero:

```groovy should-fail
t4 = timeTable("PT1S").update("X = 10 - i", "Y = Math.floorDiv((long)1, X)")
```

The table runs without error at first. After ten seconds, when `X` reaches zero, the table fails.

<details className="error">
<summary>Click to see the full stack trace.</summary>

```
aph-updateExecutor-10 | i.d.s.s.SessionService    | Internal Error 'aa3f5f3e-af03-45af-827c-f6bd8d3a5009' io.deephaven.engine.table.impl.select.FormulaEvaluationException: In formula: Y = Math.floorDiv((long)1, X)
	at io.deephaven.temp.c_acf05682d28d54381b192ae326ae1233c4dcfd572f7487269547deef388bfba7v65_0.Formula.applyFormulaPerItem(Formula.java:168)
	at io.deephaven.temp.c_acf05682d28d54381b192ae326ae1233c4dcfd572f7487269547deef388bfba7v65_0.Formula.lambda$fillChunkHelper$4(Formula.java:157)
	at io.deephaven.engine.rowset.RowSequence.lambda$forAllRowKeys$0(RowSequence.java:175)
	at io.deephaven.engine.rowset.impl.singlerange.SingleRangeMixin.forEachRowKey(SingleRangeMixin.java:17)
	at io.deephaven.engine.rowset.RowSequence.forAllRowKeys(RowSequence.java:174)
	at io.deephaven.temp.c_acf05682d28d54381b192ae326ae1233c4dcfd572f7487269547deef388bfba7v65_0.Formula.fillChunkHelper(Formula.java:155)
	at io.deephaven.temp.c_acf05682d28d54381b192ae326ae1233c4dcfd572f7487269547deef388bfba7v65_0.Formula.fillChunk(Formula.java:132)
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
	at java.base/java.lang.Thread.run(Thread.java:1583)
Caused by: java.lang.ArithmeticException: / by zero
	at java.base/java.lang.Math.floorDiv(Math.java:1535)
	at java.base/java.lang.Math.floorDiv(Math.java:1506)
	at io.deephaven.temp.c_acf05682d28d54381b192ae326ae1233c4dcfd572f7487269547deef388bfba7v65_0.Formula.applyFormulaPerItem(Formula.java:166)
	... 18 more
```

</details>

As expected for a ticking table error, the thread is `aph-updateExecutor-N` and the log entry includes a UUID. There is no reference back to the original script line.

The formula string in the error is `Y = Math.floorDiv((long)1, X)`—preserved exactly as written. The `Caused by:` chain shows the root cause: `ArithmeticException: / by zero`, thrown by `Math.floorDiv` when `X` reached zero.

> [!NOTE]
> Deephaven's query engine has special null-handling behavior that can affect how some arithmetic operations behave. For example, the standard `/` operator on integer types may return a null value rather than throwing when the divisor is zero. To reliably produce an exception on zero, use `Math.floorDiv` or similar explicit Java Math methods.

## Related documentation

- [Triage errors in queries](./triage-errors.md)
- [Groovy closures](./groovy-closures.md)
- [Formulas](./formulas.md)
- [Use filters in query strings](./use-filters.md)
- [Create and work with time tables](./time-table.md)
