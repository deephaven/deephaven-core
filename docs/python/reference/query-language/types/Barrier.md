---
title: Barrier
---

A [`Barrier`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier) is a synchronization primitive for coordinating execution order between column calculations and filters. A `Barrier` instance carries no data of its own — it is a unique marker you create once and share between the operations that need to coordinate.

## Why use a barrier?

By default, Deephaven is free to parallelize column calculations and filters that are eligible for it — eligibility depends on statelessness, table size, available threads, and, for a formula or filter that calls a Python function or uses Python objects, a free-threaded (no-GIL) Python build. A barrier lets you enforce that one operation completes all of its rows before another operation starts — for example, when one column populates a cache that another column reads from, or computes a running total that another column depends on.

A barrier alone does not force either operation to run serially. If an operation has shared mutable state that could race across its own rows, you typically need **both** [`with_serial`](./ConcurrencyControl.md#with_serial) (for sequential row processing within that operation) **and** a barrier (for ordering between operations).

## Creating a Barrier

```python syntax
from deephaven.concurrency_control import Barrier

barrier = Barrier()
```

Create one `Barrier` instance per ordering constraint you need. Reusing the same instance for unrelated constraints would incorrectly link them together; use a separate instance for each independent constraint.

## Using a barrier

One operation **declares** the barrier — it goes first. Another operation **respects** the barrier — it waits until every operation that declares that barrier has finished all of its rows. Both roles are part of the [`ConcurrencyControl`](./ConcurrencyControl.md) interface, which [`Selectable`](./Selectable.md) (used by [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md)) and [`Filter`](./Filter.md) (used by [`where`](../../table-operations/filter/where.md)) both implement:

- [`with_declared_barriers(barriers)`](./ConcurrencyControl.md#with_declared_barriers) — this operation declares the given barrier(s); it runs to completion before any operation that respects the same barrier.
- [`with_respected_barriers(barriers)`](./ConcurrencyControl.md#with_respected_barriers) — this operation respects the given barrier(s); it does not start until every operation that declares the barrier has finished.

> [!IMPORTANT]
> A barrier only coordinates expressions passed to the **same** `select`, `update`, or `where` call — it cannot order operations across two separate calls. Within that call, a respecting expression must come after the declaring expression, in left-to-right order; the engine raises an error if a barrier is respected before it is declared, or never declared at all.
>
> For a `Selectable`, a constant-valued expression cannot declare or respect a barrier either — the engine never evaluates constants during `select`/`update` processing, so it raises an error if you try. "Constant" here is narrower than "does not depend on a column or row-position variable": it means a literal, or literals combined with arithmetic/comparison operators, such as `Selectable.parse("A = 1")` or `Selectable.parse("A = 1 + 2")`. A no-argument function call like `get_and_increment_counter()` in the example below does not depend on a column or row variable either, but it is not constant — it is still evaluated once per row, so it can freely use barriers.

### Example: coordinating two columns

Consider two columns that share a counter, where column `A` should assign IDs 0-9 and column `B` should continue from 10-19. Without a barrier, the engine gives no guarantee about the order in which `A` and `B` run relative to each other — `B` could just as easily end up with 0-9 while `A` gets 10-19. A barrier removes that ambiguity: column `A` is guaranteed to run first (0-9), then column `B` starts where `A` left off (10-19):

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

`A` gets values 0-9. `B` gets values 10-19. Without the barrier, there is no guarantee `A` runs before `B` — the two columns could just as easily come out reversed. Without `with_serial`, a column's own rows could also be evaluated out of row-set order, breaking the correspondence between row and counter value even within a single column.

### Example: coordinating two filters

Barriers work the same way for [`Filter`](./Filter.md) objects in `where` operations. Here, one filter populates a cache that a second filter depends on. Neither filter needs `with_serial`: on the common GIL-enabled build, the GIL already serializes the underlying `dict` writes; on a free-threaded build, `dict`'s own internal per-object locking keeps a simple assignment to a distinct key thread-safe without extra synchronization (free-threaded CPython only requires an explicit lock for compound operations or invariants spanning more than one dict access). Either way, the barrier — not `with_serial` — is what enforces that the cache is fully populated before it is read:

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

### Multiple barriers

You can create multiple `Barrier` instances when operations have different dependencies. Each barrier is an independent constraint, and one operation can respect more than one:

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
- `D` starts after `A` finishes (does not wait for `B`).
- `C` starts after both `A` and `B` finish.

## Related documentation

- [ConcurrencyControl](./ConcurrencyControl.md) — The interface that provides `with_declared_barriers` and `with_respected_barriers`
- [Selectable](./Selectable.md) — Uses barriers to coordinate column calculations
- [Filter](./Filter.md) — Uses barriers to coordinate filters
- [Barrier Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier)
