---
title: LivenessScope
---

`LivenessScope` is a Java class (`io.deephaven.engine.liveness.LivenessScope`) that manages the [reference counting](../../conceptual/liveness-scope-concept.md) of tables and other query resources created within it. These managed objects are called the scope's referents.

To make a scope manage the objects a block of code creates, open it with [`LivenessScopeStack.open(scope, releaseOnClose)`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open(io.deephaven.engine.liveness.ReleasableLivenessManager,boolean)) in a try-with-resources block. `open` pushes the scope onto the thread's [stack of open scopes](../../conceptual/liveness-scope-concept.md#how-liveness-scopes-work) and returns a [`SafeCloseable`](/core/javadoc/io/deephaven/util/SafeCloseable.html) that pops it when the block exits, then releases it if `releaseOnClose` is `true`.

## Syntax

```groovy syntax
scope = new LivenessScope()
scope = new LivenessScope(enforceStrongReachability)
```

## Parameters

<ParamTable>
<Param name="enforceStrongReachability" type="boolean" optional>

Whether the scope maintains strong references to the referents it manages. The no-argument constructor is equivalent to `new LivenessScope(false)`: the scope does not maintain strong references to its referents.

</Param>
</ParamTable>

## Returns

A new `LivenessScope`.

## Methods

The following list covers a subset of the methods `LivenessScope` provides. For the full list, see the [Javadoc](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html).

- [`release`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html#release()): Drops this scope's references to everything it manages, unless something else still holds a reference to the scope itself. Objects that nothing else references are released. Throws an `IllegalStateException` if the scope's reference count is already zero, such as when an earlier `release` call has already dropped the scope's references.
- [`manage(referent)`](/core/javadoc/io/deephaven/engine/liveness/LivenessManager.html#manage(io.deephaven.engine.liveness.LivenessReferent)): Adds a reference from this scope to the object.
- [`unmanage(referent)`](/core/javadoc/io/deephaven/engine/liveness/LivenessManager.html#unmanage(io.deephaven.engine.liveness.LivenessReferent)): Drops one reference from this scope to the object.
- [`transferTo(other)`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html#transferTo(io.deephaven.engine.liveness.LivenessManager)): Transfers all the referents this scope retains to another [`LivenessManager`](/core/javadoc/io/deephaven/engine/liveness/LivenessManager.html). Throws an `UnsupportedOperationException` if the scope was constructed with `enforceStrongReachability` set to `true`, or if `other` is not a supported manager type.

## Examples

The following example uses [function-generated tables](../table-operations/create/create.md) to produce a refreshing table that replaces its 5 rows at most once per second. Because the example creates the function-generated table inside a liveness scope, releasing the scope drops the scope's reference to the table.

```groovy ticking-table test-set=1 order=null
import io.deephaven.engine.liveness.*
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable
import io.deephaven.engine.table.impl.util.FunctionGeneratedTableFactory

defaultCtx = ExecutionContext.getContext()


randomData = { ->
    try (SafeCloseable ignoredContext = defaultCtx.open()) {
        return emptyTable(5).update("X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)")
    }
}

scope = new LivenessScope()

try (SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
    tableFromFunc = FunctionGeneratedTableFactory.create(randomData, 1000)
}
```

The `randomData` function opens the console's [execution context](../../conceptual/execution-context.md) because the [`update`](../table-operations/select/update.md) formulas need its query compiler and query library, and the function-generated table does not capture the console's context on its own.

Because `releaseOnClose` is `false`, closing the try-with-resources block only pops the scope from the stack, and the scope still manages `tableFromFunc`.

Once `tableFromFunc` is no longer needed, call [`release`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html#release()) on `scope` to drop the scope's reference to it. The table stops updating once nothing else holds a reference to it. Another scope can hold such a reference, as can a refreshing table derived from it or an open view of the table in the UI. An open view keeps the table updating until the user closes it.

```groovy test-set=1 order=null
scope.release()
// The scope no longer keeps the table live.
// Drop the variable so the console does not hold a handle to the released table.
tableFromFunc = null
```

## Related documentation

- [Liveness Scopes](../../conceptual/liveness-scope-concept.md)
- [Execution Context](../../conceptual/execution-context.md)
- [`FunctionGeneratedTableFactory`](../table-operations/create/create.md)
- [`update`](../table-operations/select/update.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html)
