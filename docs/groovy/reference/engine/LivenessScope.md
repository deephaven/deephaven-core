---
title: LivenessScope
---

`LivenessScope` is a Groovy class that manages the reference counting of tables and other query resources that are created within it.

## Syntax

```groovy syntax
scope = new LivenessScope()
```

## Parameters

None.

## Returns

An instance of a `LivenessScope` class.

## Examples

The following example uses [function-generated tables](../table-operations/create/create.md) to produce a blink table with 5 new rows per second. Encapsulating the function-generated table inside a liveness scope enables the safe deletion of data once the scope has been released. `tableFromFunc` will continue to tick after the scope is released until _all_ referents have been deleted, including the ticking table in the UI.

```groovy ticking-table order=null
import io.deephaven.engine.liveness.*
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable
import io.deephaven.engine.table.impl.util.FunctionGeneratedTableFactory

defaultCtx = ExecutionContext.getContext()


randomData = { ->
    try ( SafeCloseable ignoredContext = defaultCtx.open()) {
        return emptyTable(5).update("X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)")
    }
}

scope = new LivenessScope()

try ( SafeCloseable ignored = LivenessScopeStack.open(scope, false) ) {
    tableFromFunc = FunctionGeneratedTableFactory.create(randomData, 1000)
}
```

## Related documentation

- [How to use Liveness Scopes](../../conceptual/liveness-scope-concept.md)
- [Execution Context](../../conceptual/execution-context.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html)
