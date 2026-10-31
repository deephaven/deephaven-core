---
title: Decipher Groovy errors
---

When you develop Groovy queries, you eventually encounter errors. This guide teaches you how to read Groovy error output so you can find and fix problems quickly. For a short checklist, see [Tips for reading errors](#tips-for-reading-errors).

## The basics

Errors in Groovy fall into two categories:

- **Compilation errors** occur before execution, when the Groovy compiler rejects your script, for example because of invalid syntax or a class it can't resolve.
- **Runtime exceptions** occur during execution, when running code throws an exception. Errors in Deephaven [formulas](./formulas.md) are runtime exceptions too, even when the message says "compilation": Deephaven compiles a [formula](./formulas.md) only when your script calls the [table operation](../conceptual/table-operations-overview.md) that uses it.

Groovy errors appear as Java exception stack traces. Each `at ...` line in a trace is a frame: one method call, with the class, file, and line number it ran from. When one exception causes another, they chain together using `Caused by:` lines.

## Read the log header

When code you run in the console fails, the console shows the stack trace. The IDE's [Log panel](./logs.md#log-panel) shows the same error as a log entry. After the time and the log level, such as `ERROR`, the entry has a header that names where the error came from:

```
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: ...
```

The header has two names, separated by `|`, and the message follows them. In the console examples below:

- `r-Scheduler-Serial-1` is the thread that reported the error. It's the end of `DeephavenApiServer-Scheduler-Serial-1`, the server thread that runs console commands.
- `.c.ConsoleServiceGrpcImpl` is the class that logged the error. It's the end of `i.d.s.c.ConsoleServiceGrpcImpl`, the server class that handles console requests.
- `Error running script` starts the message. It means the error came from running a console command.

The log shortens both names to fit fixed-width columns:

- The thread name keeps its last 20 characters.
- The class name keeps its last 25 characters. Before cutting the name, the log shortens package names to their initials, starting from the left, until the name fits or every package is an initial.

In the server's standard output, such as the [Docker container output](./logs.md#retrieve-docker-container-logs), each entry starts with the date and time in UTC, and the log level follows the thread name. Most traces on this page show only the header and the message.

## Read a Groovy compilation error

As an example, let's run a line of code with a syntax error:

```groovy should-fail
println("hello"
```

<details className="error">
<summary>Click to see the full stack trace.</summary>

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

For compilation errors, Deephaven wraps the error in a [`GroovyExceptionWrapper`](/core/javadoc/io/deephaven/engine/util/GroovyExceptionWrapper.html). The key information is at the top of the output:

```
io.deephaven.engine.util.GroovyExceptionWrapper: startup failed:
Script_6: 5: Unexpected input: ... @ line 5, column 1.
```

The line and column numbers show where the parser failed. Deephaven adds a `package io.deephaven.dynamic;` line before your code, so the reported line is one greater than the line in your editor.

In this example, the code never closes the parenthesis, so the parser keeps reading into the code that Deephaven appends after your script and fails there. As a result, the error reports line 5, which is past the end of your own code.

> [!NOTE]
> Each time you execute code in the Deephaven console, Deephaven compiles it into a new class named `Script_N`. `N` increments only when an execution compiles successfully. A compilation error doesn't advance it, so the syntax error above and the next example both use `Script_6`.
>
> This guide's traces come from more than one session, so their `Script_N` numbers don't always increase down the page. The numbers in your own traces differ from these.

The remainder of the trace shows frames from the Groovy compiler and from Deephaven's session infrastructure. You can generally ignore these frames. The useful signal is in the error message itself.

## Read a runtime exception

Now let's look at a runtime exception. Here, a [closure](./groovy-closures.md) `f` throws when its argument is `10` or `20`:

```groovy skip-test
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

Because `java.lang.Exception` is a checked exception, Deephaven wraps it in a `java.lang.RuntimeException` before reporting it. Deephaven reports unchecked exceptions as is. Either way, follow the `Caused by:` chain to find the actual error.

A line such as `... 15 more` at the end of a `Caused by:` block stands for frames that are the same as the last frames of the exception above it, so nothing is missing.

> [!TIP]
> When a Groovy stack trace has `Caused by:` lines, the last `Caused by:` entry holds the root cause. Start there and work back toward the top.

Within the `Caused by:` block, look for frames in the `io.deephaven.dynamic.Script_N` class that cite `Script_N.groovy`. These frames point to your code.

Each line number counts from the start of the console command that defined the code. As with compilation errors, each line number is one greater than the line in your editor, because of the added `package io.deephaven.dynamic;` line. Here, line 3 is the `throw` inside `f`, and line 8 is the `f(10)` call:

```
at io.deephaven.dynamic.Script_6$_run_closure1.doCall(Script_6.groovy:3)   // inside f
...
at io.deephaven.dynamic.Script_6.run(Script_6.groovy:8)                    // f(10) call
```

> [!NOTE]
> Groovy [closures](./groovy-closures.md) appear in stack traces as `Script_N$_run_closureM`, where `M` is a number based on the [closure](./groovy-closures.md)'s position in the script. This is how the Groovy compiler names anonymous [closures](./groovy-closures.md) internally. A named method would appear as `Script_N.methodName`.

Now let's add another level of depth. The [closure](./groovy-closures.md) `g` calls `f` and accumulates the results:

```groovy skip-test
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

Because you defined `f` and `g` in separate console executions, they live in different compiled classes: `Script_6` and `Script_7`. The trace crosses those boundaries, showing the full call chain from the root cause upward:

```
at io.deephaven.dynamic.Script_6$_run_closure1.doCall(Script_6.groovy:3)   // inside f
...
at io.deephaven.dynamic.Script_7$_run_closure1.doCall(Script_7.groovy:6)   // x += f(10) in g
...
at io.deephaven.dynamic.Script_7.run(Script_7.groovy:9)                    // g() call
```

## Read an undefined-variable error

A common Groovy mistake is a misspelled variable name. In this example, the second line misspells `t` as `tt`. Here, `i` is the row index, one of Deephaven's [special variables](../reference/query-language/variables/special-variables.md):

```groovy should-fail
t = emptyTable(5)
t2 = tt.update("X = i")
```

This output comes from the server's standard output, so the entry also has a timestamp and a log level, as [Read the log header](#read-the-log-header) describes. Here are the first lines of the trace:

```
2026-10-21T20:54:35.786Z | r-Scheduler-Serial-1 | ERROR | .c.ConsoleServiceGrpcImpl | Error running script: groovy.lang.MissingPropertyException: No such property: tt for class: io.deephaven.dynamic.Script_9
	at org.codehaus.groovy.runtime.ScriptBytecodeAdapter.unwrap(ScriptBytecodeAdapter.java:66)
	at org.codehaus.groovy.runtime.callsite.PogoGetPropertySite.getProperty(PogoGetPropertySite.java:51)
	at org.codehaus.groovy.runtime.callsite.AbstractCallSite.callGroovyObjectGetProperty(AbstractCallSite.java:341)
	at io.deephaven.dynamic.Script_9.run(Script_9.groovy:3)
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
```

Reading this trace:

- **No `Caused by:` line**: The full trace has none. `MissingPropertyException` is an unchecked exception, so Deephaven reports it as is, without wrapping it. The exception on the first line is the root cause.
- **Message**: `No such property: tt` names the variable that Groovy couldn't find. `Script_9` is the class that Deephaven compiled your command into.
- **Script line**: The `Script_9.groovy:3` frame points to the typo. It's one greater than the editor line (2) because Deephaven adds a `package` line before your code.

To fix the error, correct the variable name, or define the variable before you use it.

## Read a formula compilation error

Now let's look at a [formula](./formulas.md) that references a column that doesn't exist. In the following example, `f1` uses column `X` correctly, but `f2` does not.

This is not a Groovy syntax error. The Groovy code is valid, but Deephaven fails to compile the [formula](./formulas.md) `Y = X * 3` when [`update`](../reference/table-operations/select/update.md) runs. As [The basics](#the-basics) explains, the error therefore arrives as a runtime exception.

```groovy skip-test
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

Working up the trace from the last `Caused by:` entry:

1. `QueryLanguageParser$QueryLanguageParseException` — the root cause. The Deephaven [query language](./query-string-overview.md) parser failed on the [formula](./formulas.md). Its message shows the full expression, `X * 3`, and the exact problem, `Cannot find variable or class X`.
2. [`FormulaCompilationException`](/core/javadoc/io/deephaven/engine/table/impl/select/FormulaCompilationException.html) `: Formula compilation error for: X * 3` — Deephaven could not compile the [formula](./formulas.md).

The frames that point to your code are `Script_11$_run_closure2.doCall(Script_11.groovy:3)` and `Script_11.run(Script_11.groovy:5)`. The first is inside `f2`, at the [`update`](../reference/table-operations/select/update.md) call. The second is the line that calls `f2`.

The `_run_closure2` suffix marks the second [closure](./groovy-closures.md) defined in the script, which is `f2`. This confirms that `f2` caused the problem.

This is a common mistake: a [formula](./formulas.md) refers to a column that the table does not have. To fix it, create the column before the [formula](./formulas.md) that uses it, as `f1` does, or correct the column name in the [formula](./formulas.md).

## Read a formula evaluation error

Sometimes a [formula](./formulas.md) compiles, but code that it calls throws an exception during evaluation. The trace then depends on whether the table is [static](../conceptual/table-types.md#static-tables) or [ticking](../conceptual/table-types.md#standard-streaming-tables).

### Static tables

You can use [closures](./groovy-closures.md) inside table [formulas](./formulas.md). When one throws, the trace chains more exceptions than a plain [closure](./groovy-closures.md) call does.

This example reuses the [closure](./groovy-closures.md) `f` from [Read a runtime exception](#read-a-runtime-exception), which throws an exception when its argument is `10` or `20`. Here, a column [formula](./formulas.md) on a [static table](../conceptual/table-types.md#static-tables) calls `f`:

```groovy skip-test
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

Working up the trace from the last `Caused by:` entry:

1. `java.lang.Exception: oops 1` — the root cause. The [closure](./groovy-closures.md) `f` threw it.
2. [`FormulaEvaluationException`](/core/javadoc/io/deephaven/engine/table/impl/select/FormulaEvaluationException.html) `: In formula: Y = f(X)` — the query engine detected a failure while evaluating that [formula](./formulas.md).
3. [`TableInitializationException`](/core/javadoc/io/deephaven/engine/exceptions/TableInitializationException.html) `: Error while initializing Update([Y])` — the table failed to initialize because the [formula](./formulas.md) failed.

The line `Script_8.groovy:3` tells you which line in your code triggered the failing [`update`](../reference/table-operations/select/update.md) call.

### Ticking tables

A [ticking table](../conceptual/table-types.md#standard-streaming-tables) adds, modifies, or removes rows as new data arrives, so [formula](./formulas.md) errors in it behave differently.

When you apply a [formula](./formulas.md) to a [ticking table](../conceptual/table-types.md#standard-streaming-tables), Deephaven evaluates it for the existing rows when it creates the table, then again for each added row and for each modified row whose input columns changed. Errors may appear well after the table is open and apparently working.

This example reuses the [closure](./groovy-closures.md) `f` from [Read a runtime exception](#read-a-runtime-exception), which throws when its argument is `10` or `20`. It applies `f` to a [ticking table](../conceptual/table-types.md#standard-streaming-tables) from [`timeTable`](../reference/table-operations/create/timeTable.md), which adds one row per second:

```groovy skip-test
t2 = timeTable("PT1S").update("X = i")
t3 = t2.update("Y = f(X)")
```

The table runs for about ten seconds before it fails, when `X` reaches `10`.

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

Several things differ from the [static table](../conceptual/table-types.md#static-tables) error:

- **Thread**: `aph-updateExecutor-5` instead of `r-Scheduler-Serial-1`. The name is the end of `PeriodicUpdateGraph-updateExecutor-5`, a thread of the [update graph](../conceptual/periodic-update-graph-configuration.md), not the thread that runs console commands.
- **Log source**: `.AsyncClientErrorNotifier | Error in table update:` instead of `.c.ConsoleServiceGrpcImpl | Error running script`.
- **First exception**: The trace starts directly with [`FormulaEvaluationException`](/core/javadoc/io/deephaven/engine/table/impl/select/FormulaEvaluationException.html), with no [`TableInitializationException`](/core/javadoc/io/deephaven/engine/exceptions/TableInitializationException.html). Deephaven created the table successfully. The [formula](./formulas.md) failed later, when a new row arrived.
- **Script line**: No frame points to the line that created the table, because the error occurred asynchronously. The `Script_6$_run_closure1` frame still points to the `throw` inside `f`.

In the IDE, the failed table's panel is covered by a warning icon and a message that begins with `Unable to open table`. The message gives an ID. Search the [Deephaven logs](./logs.md) for that ID to find the full error.

The [next section](#find-a-ticking-table-error-by-its-log-id) shows the second log entry the server writes when a client is viewing the table, using a different [formula](./formulas.md).

### Find a ticking-table error by its log ID

When a client such as the IDE is viewing a [ticking table](../conceptual/table-types.md#standard-streaming-tables) that fails, the server writes a second log entry, `Internal Error '<id>'`, and sends the same ID to the client. The first entry is the `Error in table update` entry shown in [Ticking tables](#ticking-tables).

In this example, a [formula](./formulas.md) calls `Math.floorDiv`, which throws when its divisor reaches zero. In a Deephaven [formula](./formulas.md), `/` on integers returns a `double`, so dividing by zero gives `Infinity` instead of throwing.

```groovy skip-test
t4 = timeTable("PT1S").update("X = 10 - i", "Y = Math.floorDiv((long)1, X)")
```

The table runs without error at first. After about ten seconds, when `X` reaches zero, the table fails.

<details className="error">
<summary>Click to see the full stack trace.</summary>

```
ph-updateExecutor-10 | i.d.s.s.SessionService    | Internal Error 'aa3f5f3e-af03-45af-827c-f6bd8d3a5009' io.deephaven.engine.table.impl.select.FormulaEvaluationException: In formula: Y = Math.floorDiv((long)1, X)
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

Reading this trace:

- **Log entry**: `SessionService`, the server component that manages client sessions, writes this entry. The client receives a message that contains `Details Logged w/ID '<id>'` with the same ID, so you can search the [Deephaven logs](./logs.md) for the full error.
- **Thread**: `ph-updateExecutor-10`, the end of `PeriodicUpdateGraph-updateExecutor-10`. This is an [update graph](../conceptual/periodic-update-graph-configuration.md) thread, as in [Ticking tables](#ticking-tables).
- **Script line**: None. As in [Ticking tables](#ticking-tables), the error occurred asynchronously.
- **Formula**: The `In formula:` text shows the column name and the [formula](./formulas.md) expression, `Y = Math.floorDiv((long)1, X)`, so you can match it to your code.
- **Root cause**: `ArithmeticException: / by zero`, at the end of the `Caused by:` chain. `Math.floorDiv` threw it when `X` reached zero.

## Tips for reading errors

The examples on this page follow the same steps:

1. Find the error. Code you run in the console reports its error in the console. The full error for a [ticking table](../conceptual/table-types.md#standard-streaming-tables) appears only in the [Deephaven logs](./logs.md), such as the IDE's [Log panel](./logs.md#log-panel).
2. In the log, read the header. `Error running script` means the error happened while the console ran your code. In the [ticking-table](../conceptual/table-types.md#standard-streaming-tables) examples, the header is `Error in table update` or `Internal Error '<id>'` instead, because the table failed later, while Deephaven updated it.
3. If the IDE shows an error message with an ID, such as `Details Logged w/ID '<id>'`, search the [Deephaven logs](./logs.md) for that ID.
4. For a [`GroovyExceptionWrapper`](/core/javadoc/io/deephaven/engine/util/GroovyExceptionWrapper.html) `: startup failed:` error, read the line and column in the message. The line is one greater than the line in your editor. If it points past the end of your code, look for a parenthesis or bracket that you didn't close.
5. For other errors, find the last `Caused by:` entry. It holds the root cause. If the trace has no `Caused by:` line, the exception on the first line is the root cause.
6. Find the frames in `io.deephaven.dynamic.Script_N` classes. They point to your code. Each line number counts from the start of the console command that defined that code and is one greater than the line in your editor.
7. Match the `In formula:` or `Full expression` text to the [formula](./formulas.md) in your code.

## Related documentation

- [Deephaven's table types](../conceptual/table-types.md)
- [Table operations overview](../conceptual/table-operations-overview.md)
- [Periodic Update Graph](../conceptual/periodic-update-graph-configuration.md)
- [Query string overview](./query-string-overview.md)
- [Triage errors in queries](./triage-errors.md)
- [Retrieve logs](./logs.md)
- [Groovy closures](./groovy-closures.md)
- [Formulas](./formulas.md)
- [Create static tables](./new-and-empty-table.md)
- [Create a time table](./time-table.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [Special variables](../reference/query-language/variables/special-variables.md)
- [`GroovyExceptionWrapper` Javadoc](/core/javadoc/io/deephaven/engine/util/GroovyExceptionWrapper.html)
- [`FormulaCompilationException` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/select/FormulaCompilationException.html)
- [`FormulaEvaluationException` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/select/FormulaEvaluationException.html)
- [`TableInitializationException` Javadoc](/core/javadoc/io/deephaven/engine/exceptions/TableInitializationException.html)
