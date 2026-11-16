---
title: Liveness Scopes
---

This guide discusses liveness scopes. It covers why queries can benefit from a liveness scope, how liveness scopes work, and how to create and use one.

## Why use a liveness scope?

Deephaven's engine runs in Java, where the JVM handles [garbage collection](../how-to-guides/performance/garbage-collection.md) (GC). Users have little control over when GC runs, because the JVM decides when to collect based on memory demand.

A liveness scope lets your code choose when to release the query engine objects it no longer needs, instead of waiting for the JVM. Releasing a scope cleans up:

- Tables and plots.
- Other query engine objects, such as the listeners that connect tables in the [update graph](./dag.md#update-graph-cycles).

Without a scope of their own, queries rely on JVM [garbage collection](../how-to-guides/performance/garbage-collection.md), which is fine in most cases. A scope helps when a query repeatedly creates [ticking](./table-update-model.md) tables that it needs only for a while, such as a function that rebuilds its tables each time its parameters change. Releasing the scope cleans up the old tables right away. Otherwise they keep processing updates until [garbage collection](../how-to-guides/performance/garbage-collection.md) runs.

## How liveness scopes work

Deephaven uses _reference counting_ to decide when to clean up tables, plots, and many other query engine objects. Each object counts the references to it, and Deephaven cleans the object up as soon as the count drops to zero. An object tracked this way is a liveness _referent_. A referent is _live_ while its count is above zero. A liveness scope is an object that holds references to referents. Deephaven tracks references as follows:

- Open liveness scopes form a stack on each thread. When a query creates a referent, the scope at the top of the stack manages it, which adds a reference. While the Python console runs your code, the console's own liveness scope is open, so by default it manages the tables and other referents your script creates.
- A [refreshing](./table-update-model.md) (ticking) table holds a reference to each refreshing parent table it depends on, so a parent stays live as long as one of its children does.
- When a referent's reference count drops to zero, Deephaven releases it immediately. A released refreshing table stops processing updates and drops its references to its parents, which can in turn be released.
- Releasing a scope drops its references to everything it manages. Referents that nothing else references are released. The rest, such as a result you [preserve](#scope-methods) in the enclosing scope (the scope below it on the stack), stay live.

Reference counting does not replace [GC](../how-to-guides/performance/garbage-collection.md). A variable that refers to a table is not one of these counted references. Suppose no variable refers to a refreshing table, no live table depends on it, and no scope that manages it has been released. The table keeps processing updates until the JVM [garbage collector](../how-to-guides/performance/garbage-collection.md) reclaims it.

## How to create and use a liveness scope

Creating a liveness scope is easy. Call the [`liveness_scope`](../reference/engine/liveness-scope.md) function, or create a scope directly from the [`LivenessScope`](../reference/engine/LivenessScope.md) class. Neither takes arguments.

- Use the [`liveness_scope`](../reference/engine/liveness-scope.md) function _only_ in a [`with`](https://peps.python.org/pep-0343/) block or as a function decorator.
- The [`LivenessScope`](../reference/engine/LivenessScope.md) class gives you finer control, because you can open a scope more than once and release the objects it manages when you choose.

Use the function when a scope covers one block or one function call. Use the class when you need to release the scope later, from code outside the block that created it.

```python order=null
from deephaven.liveness_scope import liveness_scope, LivenessScope

with liveness_scope() as scope_from_function:
    pass

scope_from_class = LivenessScope()
scope_from_class.release()
```

### The `liveness_scope` function

Entering a [`with liveness_scope()`](../reference/engine/liveness-scope.md) block, or calling a function decorated with [`@liveness_scope()`](../reference/engine/liveness-scope.md), creates a [`SimpleLivenessScope`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.SimpleLivenessScope), which can only be opened once. While it is the innermost open scope, the scope manages the query engine objects your code creates, and it drops its references to them when the block or decorated function exits. Use [`preserve`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.SimpleLivenessScope.preserve) to keep a result live in the enclosing scope.

The following example builds the same ticking query in two functions:

- `get_latest` preserves the table it returns, so `latest_values` keeps ticking after the `with` block exits. Its parent, the `ticking_table` in `get_latest`, also stays live, because the preserved table depends on it.
- `get_values` preserves nothing, so its scope releases every table it creates when the function returns. Only the NumPy array, which is a copy of the data, remains.

```python ticking-table order=null
import datetime
from deephaven import time_table
from deephaven import numpy as dhnp
from deephaven.liveness_scope import liveness_scope


def get_latest():
    with liveness_scope() as scope:
        ticking_table = time_table("PT1s").update(
            ["Sym = (ii % 2 == 0) ? `A` : `B`", "Value = ii"]
        )
        latest = ticking_table.last_by("Sym")
        scope.preserve(latest)
    return latest


@liveness_scope()
def get_values():
    # Start 10 seconds in the past so the table already has rows to copy
    start = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        seconds=10
    )
    ticking_table = (
        time_table("PT1s", start_time=start)
        .update(["Sym = (ii % 2 == 0) ? `A` : `B`", "Value = ii"])
        .last_by("Sym")
    )
    return dhnp.to_numpy(ticking_table, cols=["Value"])


latest_values = get_latest()
values = get_values()
```

### The `LivenessScope` class

Use the [`LivenessScope`](../reference/engine/LivenessScope.md) class when the scope must outlive one `with` block and you want to choose when to release it.

In the following example, each call to `make_table_and_scope` creates a [filtered](../reference/table-operations/filter/where.md) ticking table inside a new scope and returns both. The scope stays unreleased after the `with` block, so the table keeps ticking. Releasing the scope stops the table, even though `t1` still refers to it. Two things keep it updating after the release:

- Another scope, or a refreshing child table, that holds a reference to it.
- The UI, while the table is open there.

```python ticking-table order=null
from deephaven import time_table
from deephaven.liveness_scope import LivenessScope


def make_table_and_scope(a: int):
    scope = LivenessScope()
    with scope.open():
        ticking_table = time_table("PT1s").update("A = ii % 3").where(f"A = {a}")
        return ticking_table, scope


t1, s1 = make_table_and_scope(1)
# ... wait for a while
s1.release()
# The released table no longer updates. Drop the variable too, so nothing refers to it.
t1 = None
t2, s2 = make_table_and_scope(2)
# ... wait for a while again
s2.release()
t2 = None
```

## Multiple liveness scopes

Liveness scopes can be nested. Because open scopes form a [stack](#how-liveness-scopes-work), the innermost open scope manages new objects. When its `with` block exits, the scope that encloses it manages new objects again.

In the following example, the inner scope manages `ticks` and `evens`. Inside the inner block, [`preserve`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.SimpleLivenessScope.preserve) hands `ticks` to `outer_scope`. Inside the outer block, [`preserve`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.SimpleLivenessScope.preserve) hands it on to the console's scope.

```python ticking-table order=null
from deephaven import time_table
from deephaven.liveness_scope import liveness_scope

with liveness_scope() as outer_scope:
    with liveness_scope() as inner_scope:
        ticks = time_table("PT1s").update("X = ii")
        evens = ticks.where("X % 2 == 0")
        inner_scope.preserve(ticks)
    # evens is released here, and outer_scope now manages ticks
    outer_scope.preserve(ticks)

# The released table no longer updates. Drop the variable too, so nothing refers to it.
evens = None
```

`evens` is released when the inner block exits, so it stops updating. `ticks` keeps ticking, because each scope preserved it before it closed.

## Scope methods

Scopes created by both the [`liveness_scope`](../reference/engine/liveness-scope.md) function and the [`LivenessScope`](../reference/engine/LivenessScope.md) class have these methods:

- [`manage(referent)`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope.manage) adds a reference from this scope to the object, so the object stays live until the scope is released.
- [`preserve(referent)`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope.preserve) keeps the object live in the enclosing scope. The scope must be open and at the top of the stack when you call it.
- [`unmanage(referent)`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope.unmanage) drops one reference from this scope to the object.

Scopes created directly from the [`LivenessScope`](../reference/engine/LivenessScope.md) class also have these methods:

- [`open`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope.open) opens the scope for use in a `with` statement.
- [`release`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope.release) closes the scope and drops its references to everything it manages.

## Example: release a query's intermediate tables

Queries often create intermediate tables that you don't need to refer to once the query has run. Without a liveness scope of their own, those tables stay managed by the console's scope. Let's look at the problem first, and then solve it with a scope.

This query uses [`read_csv`](../reference/data-import-export/CSV/readCsv.md), [`merge`](../reference/table-operations/merge/merge.md), and [`tree`](../reference/table-operations/create/tree.md) to create a simple [tree table](../how-to-guides/tree-table.md) grouped by `Instrument`. Two tables remain in the console: `crypto` and `combo_tree`.

```python order=combo_tree,crypto
from deephaven.csv import read as read_csv
from deephaven import merge

crypto = (
    read_csv(
        "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
    )
    .first_by("Instrument")
    .update(["ID = Instrument", "Parent = (String)null"])
    .update(
        [
            "Timestamp = (Instant)null",
            "Exchange = (String)null",
            "Price = (Double)null",
            "Size = (Double)null",
        ]
    )
)

data = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
).update(["ID = String.valueOf(ii)", "Parent = Instrument"])

combo = merge([crypto, data])

combo_tree = combo.tree("ID", "Parent")

data = None
combo = None
```

The above example works, but the query creates many objects that the console doesn't need to refer to by name once the query finishes: `data`, `combo`, and the unnamed table that each chained operation produces. Setting `data` and `combo` to `None` removes their names from the console, but it doesn't release the tables.

The following example runs the same query inside a [`LivenessScope`](../reference/engine/LivenessScope.md). Inside the scope, [`preserve`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope.preserve) keeps `crypto` and `combo_tree` live in the console's scope. Calling [`release`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope.release) then releases the intermediate tables, because no other scope or table manages them. The last two lines remove the released tables' names from the console.

```python order=combo_tree,crypto
from deephaven.liveness_scope import LivenessScope
from deephaven.csv import read as read_csv
from deephaven import merge

scope = LivenessScope()

with scope.open():
    crypto = (
        read_csv(
            "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
        )
        .first_by("Instrument")
        .update(["ID = Instrument", "Parent = (String)null"])
        .update(
            [
                "Timestamp = (Instant)null",
                "Exchange = (String)null",
                "Price = (Double)null",
                "Size = (Double)null",
            ]
        )
    )

    data = read_csv(
        "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv"
    ).update(["ID = String.valueOf(ii)", "Parent = Instrument"])

    combo = merge([crypto, data])

    combo_tree = combo.tree("ID", "Parent")

    # Keep the tables the console displays live after the scope is released
    scope.preserve(crypto)
    scope.preserve(combo_tree)

scope.release()

data = None
combo = None
```

The release succeeds, but it frees no memory here. Every intermediate table is an ancestor of `combo_tree`, which still reads their data. Because [`read_csv`](../reference/data-import-export/CSV/readCsv.md) reads the file once, these tables are static and have no updates to stop. If they were refreshing, `combo_tree` would keep them live and updating. A scope frees resources only for tables that nothing you keep depends on.

The next example shows that case with ticking data. `latest` is the result you keep, and it reads from `trades`. `every_third` also reads from `trades`, but nothing you keep reads from `every_third`.

```python ticking-table order=null
from deephaven import time_table
from deephaven.liveness_scope import LivenessScope

scope = LivenessScope()

with scope.open():
    trades = time_table("PT1s").update(
        ["Sym = (ii % 2 == 0) ? `A` : `B`", "Price = ii"]
    )
    latest = trades.last_by("Sym")
    every_third = trades.where("Price % 3 == 0")
    scope.preserve(latest)

# ... later, when you no longer need every_third
scope.release()
# The released table no longer updates. Drop the variable too, so nothing refers to it.
every_third = None
```

After [`release`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope.release), `every_third` stops updating, because nothing you kept depends on it. `latest` and its parent `trades` keep ticking, because the preserved `latest` depends on `trades`. As in the [`LivenessScope` class](#the-livenessscope-class) example, a table that is open in the UI keeps updating until you close it.

## Related documentation

- [Directed acyclic graph (DAG)](./dag.md)
- [Incremental update model](./table-update-model.md)
- [Run garbage collection](../how-to-guides/performance/garbage-collection.md)
- [Create a hierarchical tree table](../how-to-guides/tree-table.md)
- [`LivenessScope`](../reference/engine/LivenessScope.md)
- [`liveness_scope`](../reference/engine/liveness-scope.md)
- [`first_by`](../reference/table-operations/group-and-aggregate/firstBy.md)
- [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md)
- [`merge`](../reference/table-operations/merge/merge.md)
- [`read_csv`](../reference/data-import-export/CSV/readCsv.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`to_numpy`](../reference/numpy/to-numpy.md)
- [`tree`](../reference/table-operations/create/tree.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
- [Pydoc](/core/pydoc/code/deephaven.liveness_scope.html#module-deephaven.liveness_scope)
