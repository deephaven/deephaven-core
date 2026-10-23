---
title: Liveness Scopes
---

This guide discusses liveness scopes. It covers why queries can benefit from a liveness scope, how liveness scopes work, and how to create and use one.

## Why use a liveness scope?

Deephaven's engine runs in Java, where the JVM handles [garbage collection](./dag.md#garbage-collection-and-the-dag) (GC). Users have little control over when GC runs, because the JVM decides when to collect based on memory demand.

A liveness scope lets your code choose when to release the query engine objects it no longer needs, instead of waiting for the JVM. Releasing a scope cleans up:

- Tables and plots.
- Other query engine objects, such as the listeners that connect tables in the [update graph](./dag.md#update-graph-ug-cycles).

Without a scope of their own, queries rely on JVM [garbage collection](./dag.md#garbage-collection-and-the-dag), which is fine in most cases. A scope helps when a query repeatedly creates [ticking](./table-update-model.md) tables that it needs only for a while, such as a script that rebuilds its tables each time its parameters change. Releasing the scope cleans up the old tables right away. Otherwise they keep processing updates until [garbage collection](./dag.md#garbage-collection-and-the-dag) runs.

## How liveness scopes work

Deephaven uses _reference counting_ to decide when to clean up tables, plots, and many other query engine objects. Each object counts the references to it, and Deephaven cleans the object up as soon as the count drops to zero. An object tracked this way is a liveness _referent_. A referent is _live_ while its count is above zero. A liveness scope is an object that holds references to referents. Deephaven tracks references as follows:

- Open liveness scopes form a stack on each thread. When a query creates a referent, the scope at the top of the stack manages it, which adds a reference. While the Groovy console runs your code, the console's own liveness scope is open, so by default it manages the tables and other referents your script creates.
- A [refreshing](./table-update-model.md) (ticking) table holds a reference to each refreshing parent table it depends on, so a parent stays live as long as one of its children does.
- When a referent's reference count drops to zero, Deephaven releases it immediately. A released refreshing table stops processing updates and drops its references to its parents, which can in turn be released.
- Releasing a scope drops its references to everything it manages. Referents that nothing else references are released. The rest, such as a result you [manage in an outer scope](#scope-and-stack-methods) (one lower on the stack), stay live.

Reference counting does not replace [GC](./dag.md#garbage-collection-and-the-dag). A variable that refers to a table is not one of these counted references. Suppose no variable refers to a refreshing table, no live table depends on it, and no scope that manages it has been released. The table keeps processing updates until the JVM [garbage collector](./dag.md#garbage-collection-and-the-dag) reclaims it.

## How to use a `LivenessScope`

In Groovy, you work with two classes. [`LivenessScope`](../reference/engine/LivenessScope.md) is the scope itself. [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) holds each thread's stack of open scopes.

Create a [`LivenessScope`](../reference/engine/LivenessScope.md), then enclose your query in a try-with-resources block that calls [`LivenessScopeStack.open(scope, releaseOnClose)`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open(io.deephaven.engine.liveness.ReleasableLivenessManager,boolean)). While it is the innermost open scope, the scope manages the objects your query creates inside the block. When the block exits:

- If `releaseOnClose` is `true`, closing the block pops the scope from the stack and releases it.
- If `releaseOnClose` is `false`, closing the block only pops the scope from the stack. Call [`scope.release`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html#release()) yourself when you no longer need its objects.

```groovy order=null
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

scope = new LivenessScope()

try ( SafeCloseable ignored = LivenessScopeStack.open(scope, true) ) {

    // Your query here

}
```

In the following example, each call to `makeTableAndScope` creates a [filtered](../reference/table-operations/filter/where.md) ticking table inside a new scope and returns both. Because `releaseOnClose` is `false`, the scope stays unreleased after the block, so the table keeps ticking. Calling [`release`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html#release()) stops the table, even though `t1` still refers to it. Two things keep it updating after the release:

- Another scope, or a refreshing child table, that holds a reference to it.
- The UI, while the table is open there.

```groovy ticking-table order=null
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

makeTableAndScope = { int a ->
    def scope = new LivenessScope()
    def table
    try ( SafeCloseable ignored = LivenessScopeStack.open(scope, false) ) {
        table = timeTable("PT1s").update("A = ii % 3").where("A = " + a)
    }
    return [table, scope]
}

result1 = makeTableAndScope(1)
t1 = result1[0]
// ... wait for a while
result1[1].release()
// The released table no longer updates. Drop the variable too, so nothing refers to it.
t1 = null
result1 = null
result2 = makeTableAndScope(2)
t2 = result2[0]
// ... wait for a while again
result2[1].release()
t2 = null
result2 = null
```

Call [`LivenessScopeStack.open`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open()) with no arguments to create an anonymous scope, one that [`open`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open()) creates for you and that you hold no variable for. It is released when the block exits.

```groovy order=null
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

try ( SafeCloseable ignored = LivenessScopeStack.open() ) {

    // Your query here, managed by an anonymous scope that is released when the block exits

}
```

### Push and pop a scope manually

The following example calls [`LivenessScopeStack.push(scope)`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#push(io.deephaven.engine.liveness.LivenessManager)) and [`LivenessScopeStack.pop(scope)`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#pop(io.deephaven.engine.liveness.LivenessManager)) directly to manage a [`LivenessScope`](../reference/engine/LivenessScope.md). This does by hand what [`LivenessScopeStack.open(scope, true)`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open(io.deephaven.engine.liveness.ReleasableLivenessManager,boolean)) does in a try-with-resources block:

```groovy order=null
import io.deephaven.engine.liveness.*

// Create a new LivenessScope
scope = new LivenessScope()

// Add scope to the LivenessScopeStack. This makes the scope the current scope for the current thread.
LivenessScopeStack.push(scope)

// Your query here

// Remove the scope from the stack. This does not release anything the scope manages.
LivenessScopeStack.pop(scope)

// Release the objects the scope manages.
scope.release()
```

> [!NOTE]
> This example shows how the [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) manages [`LivenessScope`](../reference/engine/LivenessScope.md) objects. In practice, use try-with-resources blocks, which pop the scope even if the query throws an exception.

## Multiple liveness scopes

Liveness scopes can be nested. Because open scopes form a [stack](#how-liveness-scopes-work), the scope at the top of the [`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) manages new objects. When that scope is popped, the scope below it is at the top again. Nested try-with-resources blocks push and pop scopes in the right order.

In the following example, the inner anonymous scope manages `ticks` and `evens`. Before each block opens, [`LivenessScopeStack.peek`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#peek()) gets the scope that encloses it: `consoleScope` for the outer block and `outerScope` for the inner block. Inside the inner block, [`outerScope.manage`](/core/javadoc/io/deephaven/engine/liveness/LivenessManager.html#manage(io.deephaven.engine.liveness.LivenessReferent)) keeps `ticks` live in the outer scope. Inside the outer block, [`consoleScope.manage`](/core/javadoc/io/deephaven/engine/liveness/LivenessManager.html#manage(io.deephaven.engine.liveness.LivenessReferent)) keeps it live in the console's scope.

```groovy ticking-table order=null
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

// The console's scope
consoleScope = LivenessScopeStack.peek()

try ( SafeCloseable ignored = LivenessScopeStack.open() ) {
    outerScope = LivenessScopeStack.peek()
    try ( SafeCloseable ignored2 = LivenessScopeStack.open() ) {
        ticks = timeTable("PT1s").update("X = ii")
        evens = ticks.where("X % 2 == 0")
        outerScope.manage(ticks)
    }
    // evens is released here, and outerScope still manages ticks
    consoleScope.manage(ticks)
}

// The released table no longer updates. Drop the variable too, so nothing refers to it.
evens = null
```

`evens` is released when the inner block exits, so it stops updating. `ticks` keeps ticking, because `outerScope` manages it before the inner scope is released, and the console's scope manages it before `outerScope` is released.

## Scope and stack methods

[`LivenessScopeStack`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html) methods control which scope manages new objects on the current thread:

- [`LivenessScopeStack.push(scope)`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#push(io.deephaven.engine.liveness.LivenessManager)) pushes a scope onto the current thread's scope stack, which makes it the scope that manages new objects.
- [`LivenessScopeStack.pop(scope)`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#pop(io.deephaven.engine.liveness.LivenessManager)) pops the scope from the top of the current thread's scope stack. The scope must be at the top of the stack. Popping a scope does not release it.
- [`LivenessScopeStack.open(scope, releaseOnClose)`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open(io.deephaven.engine.liveness.ReleasableLivenessManager,boolean)) pushes a scope onto the scope stack and returns a [`SafeCloseable`](/core/javadoc/io/deephaven/util/SafeCloseable.html) that pops it. If `releaseOnClose` is `true`, closing the [`SafeCloseable`](/core/javadoc/io/deephaven/util/SafeCloseable.html) also releases the scope.
- [`LivenessScopeStack.open`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#open()) with no arguments pushes an anonymous scope onto the scope stack and returns a [`SafeCloseable`](/core/javadoc/io/deephaven/util/SafeCloseable.html) that pops and releases it.
- [`LivenessScopeStack.peek`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#peek()) gets the scope that manages new objects on the current thread, which is the scope at the top of the stack. If the thread has no open scope, it returns the thread's base manager, which never releases the objects it manages.

Scope methods control which objects a scope keeps live:

- [`scope.manage(referent)`](/core/javadoc/io/deephaven/engine/liveness/LivenessManager.html#manage(io.deephaven.engine.liveness.LivenessReferent)) adds a reference from `scope` to the referent. To keep a result live after an inner scope is released, call it on an outer scope, such as the one [`LivenessScopeStack.peek`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#peek()) returns before the inner scope opens.
- [`scope.unmanage(referent)`](/core/javadoc/io/deephaven/engine/liveness/LivenessManager.html#unmanage(io.deephaven.engine.liveness.LivenessReferent)) drops one reference from `scope` to the referent.
- [`scope.release`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html#release()) releases the scope, dropping its references to everything it manages.

## Example: release a query's intermediate tables

Queries often create intermediate tables that you don't need to refer to once the query has run. Without a liveness scope of their own, those tables stay managed by the console's scope. Let's look at the problem first, and then solve it with a scope.

This query uses [`readCsv`](../reference/data-import-export/CSV/readCsv.md), [`merge`](../reference/table-operations/merge/merge.md), and [`tree`](../reference/table-operations/create/tree.md) to create a simple [tree table](../how-to-guides/tree-table.md) grouped by `Instrument`. Two tables remain in the console: `crypto` and `comboTree`.

```groovy order=comboTree,crypto
import static io.deephaven.csv.CsvTools.readCsv

crypto = readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
)
    .firstBy("Instrument")
    .update("ID = Instrument", "Parent = (String)null")
    .update("Timestamp = (Instant)null", "Exchange = (String)null", "Price = (Double)null", "Size = (Double)null")

data = readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
)
    .update("ID = Long.toString(ii)", "Parent = Instrument")

combo = merge(crypto, data)

comboTree = combo.tree("ID", "Parent")

data = null

combo = null
```

The above example works, but the query creates many objects that the console doesn't need to refer to by name once the query finishes: `data`, `combo`, and the unnamed table that each chained operation produces. Setting `data` and `combo` to `null` removes their names from the console, but it doesn't release the tables.

The following example runs the same query inside a [`LivenessScope`](../reference/engine/LivenessScope.md) that is released when the block exits. Before the block, [`LivenessScopeStack.peek`](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html#peek()) gets the console's scope. Inside the block, [`enclosing.manage`](/core/javadoc/io/deephaven/engine/liveness/LivenessManager.html#manage(io.deephaven.engine.liveness.LivenessReferent)) keeps `crypto` and `comboTree` live in that scope. When the block exits, the scope is released, and so are the intermediate tables, because no other scope or table manages them. The last two lines remove the released tables' names from the console.

```groovy order=comboTree,crypto
import io.deephaven.engine.liveness.*
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.util.SafeCloseable

scope = new LivenessScope()

// The console's scope, which manages the tables the console displays
enclosing = LivenessScopeStack.peek()

try ( SafeCloseable ignored = LivenessScopeStack.open(scope, true) ) {

    crypto = readCsv(
        "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
    )
        .firstBy("Instrument")
        .update("ID = Instrument", "Parent = (String)null")
        .update("Timestamp = (Instant)null", "Exchange = (String)null", "Price = (Double)null", "Size = (Double)null")

    data = readCsv(
        "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
    )
        .update("ID = Long.toString(ii)", "Parent = Instrument")

    combo = merge(crypto, data)

    comboTree = combo.tree("ID", "Parent")

    // Keep the tables the console displays live after the scope is released
    enclosing.manage(crypto)
    enclosing.manage(comboTree)

}

data = null

combo = null
```

The release succeeds, but it frees no memory here. Every intermediate table is an ancestor of `comboTree`, which still reads their data. Because [`readCsv`](../reference/data-import-export/CSV/readCsv.md) reads the file once, these tables are static and have no updates to stop. If they were refreshing, `comboTree` would keep them live and updating. A scope frees resources only for tables that nothing you keep depends on.

The next example shows that case with ticking data. `latest` is the result you keep, and it reads from `trades`. `everyThird` also reads from `trades`, but nothing you keep reads from `everyThird`.

```groovy ticking-table order=null
import io.deephaven.engine.liveness.*
import io.deephaven.util.SafeCloseable

scope = new LivenessScope()

// The console's scope
consoleScope = LivenessScopeStack.peek()

try ( SafeCloseable ignored = LivenessScopeStack.open(scope, false) ) {
    trades = timeTable("PT1s").update("Sym = (ii % 2 == 0) ? `A` : `B`", "Price = ii")
    latest = trades.lastBy("Sym")
    everyThird = trades.where("Price % 3 == 0")
    consoleScope.manage(latest)
}

// ... later, when you no longer need everyThird
scope.release()
// The released table no longer updates. Drop the variable too, so nothing refers to it.
everyThird = null
```

After [`release`](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html#release()), `everyThird` stops updating, because nothing you kept depends on it. `latest` and its parent `trades` keep ticking, because the console's scope manages `latest`, which depends on `trades`. As in the [`makeTableAndScope` example](#how-to-use-a-livenessscope), a table that is open in the UI keeps updating until you close it.

## Related documentation

- [Directed acyclic graph (DAG)](./dag.md)
- [Incremental update model](./table-update-model.md)
- [Create a hierarchical tree table](../how-to-guides/tree-table.md)
- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`firstBy`](../reference/table-operations/group-and-aggregate/firstBy.md)
- [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md)
- [`merge`](../reference/table-operations/merge/merge.md)
- [`readCsv`](../reference/data-import-export/CSV/readCsv.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`tree`](../reference/table-operations/create/tree.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`LivenessScope` Javadoc](/core/javadoc/io/deephaven/engine/liveness/LivenessScope.html)
- [`LivenessManager` Javadoc](/core/javadoc/io/deephaven/engine/liveness/LivenessManager.html)
- [`LivenessScopeStack` Javadoc](/core/javadoc/io/deephaven/engine/liveness/LivenessScopeStack.html)
- [`SafeCloseable` Javadoc](/core/javadoc/io/deephaven/util/SafeCloseable.html)
