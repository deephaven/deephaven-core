---
title: Barrier
---

A [`Barrier`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier) is a marker object that orders one column calculation or filter before another. One operation **declares** the [Barrier](#creating-a-barrier) and runs first. Other operations **respect** the [Barrier](#creating-a-barrier) and wait until the declaring operation has finished all of its rows.

A [Barrier](#creating-a-barrier) carries no data of its own. You create it once and share it between the operations that need to coordinate.

## Why use a Barrier?

Deephaven parallelizes column calculations and filters that are eligible for it. Eligibility depends on these factors:

- Whether the operation is stateless. Deephaven assumes formulas and filters are safe to run in parallel unless they are marked serial with [`with_serial`](./ConcurrencyControl.md#with_serial).
- How many rows the operation processes.
- How many threads are available.
- For a formula or filter that calls a Python function or uses Python objects, whether the Python build is free-threaded (no GIL). On a non-free-threaded build, Deephaven never splits a Python-backed column or filter across cores.

As a result, two eligible operations run at the same time, with no guaranteed order between them. A [Barrier](#creating-a-barrier) makes one operation finish all of its rows before another starts. Typical cases are one column that populates a cache that another column reads, or one column that computes a running total that another column depends on.

## Creating a Barrier

```python syntax
from deephaven.concurrency_control import Barrier

barrier = Barrier()
```

A [Barrier](#creating-a-barrier) wraps a plain Java `Object` and holds no state.

Create one [Barrier](#creating-a-barrier) for each ordering constraint. Every expression that respects a [Barrier](#creating-a-barrier) waits for its single declarer, so one instance cannot express two independent constraints.

## Using a Barrier

Two methods set the roles. Both belong to the [`ConcurrencyControl`](./ConcurrencyControl.md) interface, which [`Selectable`](./Selectable.md) (used by [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md)) and [`Filter`](./Filter.md) (used by [`where`](../../table-operations/filter/where.md)) both implement:

- [`with_declared_barriers(barriers)`](./ConcurrencyControl.md#with_declared_barriers): this operation declares the given [Barrier](#creating-a-barrier) objects. It runs to completion before any operation that respects the same [Barrier](#creating-a-barrier).
- [`with_respected_barriers(barriers)`](./ConcurrencyControl.md#with_respected_barriers): this operation respects the given [Barrier](#creating-a-barrier) objects. It does not start until the operation that declares each [Barrier](#creating-a-barrier) has finished.

These rules apply to every [Barrier](#creating-a-barrier):

- One expression declares a given [Barrier](#creating-a-barrier). Declaring the same instance twice is an error.
- The declaring and respecting expressions are arguments of the same [`select`](../../table-operations/select/select.md), [`update`](../../table-operations/select/update.md), or [`where`](../../table-operations/filter/where.md) call. A [Barrier](#creating-a-barrier) cannot order operations across two separate calls.
- The declaring expression comes before every respecting expression, in left-to-right order. The engine raises an error if a [Barrier](#creating-a-barrier) is respected before it is declared, or never declared at all.
- A constant-valued [`Selectable`](./Selectable.md) cannot declare or respect a [Barrier](#creating-a-barrier). This restriction does not apply to a [`Filter`](./Filter.md).

For the last rule, the engine never evaluates constants during [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md) processing, so it raises an error if you try. A constant is a literal, or literals joined by binary operators (arithmetic, comparison, logical, or bitwise), optionally parenthesized, such as [`Selectable.parse`](./Selectable.md#creating-a-selectable) `("A = 1")` or [`Selectable.parse`](./Selectable.md#creating-a-selectable) `("A = 1 + 2")`.

An expression that calls a function with no arguments, such as `get_and_increment_counter()` in the example below, is not constant. The engine evaluates it for each row, so it can declare and respect [Barriers](#creating-a-barrier).

### Barriers and serial execution

A [Barrier](#creating-a-barrier) orders operations relative to each other. It does not change how one operation processes its own rows.

[`with_serial`](./ConcurrencyControl.md#with_serial) makes one operation process its rows one at a time, in row set order. An operation whose own rows could race on shared mutable state needs `with_serial`, and it also needs a [Barrier](#creating-a-barrier) when another operation must wait for it.

### Example: coordinating two columns

Two columns share a counter. Column `A` assigns IDs 0-9, and column `B` continues from 10-19.

Without a [Barrier](#creating-a-barrier), the engine guarantees no order between `A` and `B`, so `B` could end up with 0-9 while `A` gets 10-19. With a [Barrier](#creating-a-barrier), column `A` runs first and column `B` starts where `A` left off. Each column also uses [`with_serial`](./ConcurrencyControl.md#with_serial), because the counter is shared mutable state:

```python order=t
from deephaven.concurrency_control import Barrier
from deephaven.table import Selectable
from deephaven import empty_table

counter = 0


def get_and_increment_counter() -> int:
    global counter
    ret = counter
    counter += 1
    return ret


barrier = Barrier()

# Column A: serial (protect counter) + declares barrier (must finish first)
col_a = (
    Selectable.parse("A = get_and_increment_counter()")
    .with_serial()
    .with_declared_barriers(barrier)
)

# Column B: serial (protect counter) + respects barrier (waits for A)
col_b = (
    Selectable.parse("B = get_and_increment_counter()")
    .with_serial()
    .with_respected_barriers(barrier)
)

t = empty_table(10).update([col_a, col_b])
```

`A` gets values 0-9 and `B` gets values 10-19. The [Barrier](#creating-a-barrier) guarantees that `A` finishes before `B` starts, and [`with_serial`](./ConcurrencyControl.md#with_serial) keeps each column's rows in row set order, so each row gets the counter value that matches its position.

### Example: coordinating two filters

[`Filter`](./Filter.md) objects use [Barriers](#creating-a-barrier) in [`where`](../../table-operations/filter/where.md) the same way as columns do. Here, one filter populates a cache that a second filter reads.

Neither filter needs [`with_serial`](./ConcurrencyControl.md#with_serial). On a GIL-enabled Python build, the GIL serializes the `dict` writes. On a free-threaded build, the `dict` locks itself internally, so assigning to a distinct key is thread-safe. Free-threaded CPython requires an explicit lock only for compound operations or invariants that span more than one `dict` access.

On a free-threaded Python build, the [Barrier](#creating-a-barrier) is what ensures the cache is fully populated before it is read. On a non-free-threaded build, both filters call Python and run in argument order, so the [Barrier](#creating-a-barrier) does not change the result. Because `filter_a` finishes all ten rows first, `use_cache` finds every key and `result` keeps all ten rows:

```python order=result
from deephaven.concurrency_control import Barrier
from deephaven.filters import Filter
from deephaven import empty_table

cache = {}


def init_cache(key) -> bool:
    cache[key] = f"Value_{key}"
    return True


def use_cache(key) -> bool:
    return key in cache


barrier = Barrier()

# Filter A: declares barrier (must finish first)
filter_a = Filter.from_("(boolean)init_cache(Key)").with_declared_barriers(barrier)

# Filter B: respects barrier (waits for A to finish populating the cache)
filter_b = Filter.from_("(boolean)use_cache(Key)").with_respected_barriers(barrier)

source = empty_table(10).update("Key = i")
result = source.where([filter_a, filter_b])
```

### Multiple Barriers

Create one [Barrier](#creating-a-barrier) for each independent dependency. One operation can respect more than one [Barrier](#creating-a-barrier):

```python order=t
from deephaven.concurrency_control import Barrier
from deephaven.table import Selectable
from deephaven import empty_table

barrier_a = Barrier()
barrier_b = Barrier()

# Column A declares barrier_a
col_a = Selectable.parse("A = i * 2").with_declared_barriers(barrier_a)

# Column B declares barrier_b
col_b = Selectable.parse("B = i * 3").with_declared_barriers(barrier_b)

# Column C respects BOTH barriers — waits for A and B to finish
col_c = Selectable.parse("C = i * 4").with_respected_barriers([barrier_a, barrier_b])

# Column D respects only barrier_a — waits for A, but not B
col_d = Selectable.parse("D = i * 5").with_respected_barriers(barrier_a)

t = empty_table(10).update([col_a, col_b, col_c, col_d])
```

Execution order:

- `A` and `B` do not depend on each other, so the engine is free to run them concurrently.
- `D` starts after `A` finishes. It does not wait for `B`.
- `C` starts after both `A` and `B` finish.

## Implicit Barriers

An implicit [Barrier](#creating-a-barrier) is one that Deephaven adds for you. When [implicit Barriers](../../../conceptual/query-engine/parallelization.md#implicit-barriers) are turned on, each serial column in a [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md) waits for every earlier serial column in the same operation, without any explicit [Barrier](#creating-a-barrier) object.

[Implicit Barriers](../../../conceptual/query-engine/parallelization.md#implicit-barriers) are off by default. The [`QueryTable.serialSelectImplicitBarriers`](../../../conceptual/query-table-configuration.md#stateless-by-default) property turns them on.

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Query table configuration](../../../conceptual/query-table-configuration.md)
- [ConcurrencyControl](./ConcurrencyControl.md)
- [Selectable](./Selectable.md)
- [Filter](./Filter.md)
- [Barrier Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier)
