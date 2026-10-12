---
title: Parallelization
---

<!-- Excerpt of docs/python/conceptual/query-engine/parallelization.md at 609b266973 (PR #7457), a Concept guide. Passages are verbatim; gaps are marked. -->

### Query phases and thread pools

Queries execute in two phases, and Deephaven uses a separate thread pool for each.

**Initialization**: Every table operation is computed once when you call it — whether at the top of a script or later in a running console. Deephaven computes the initial result from all existing data, splitting the rows across cores as described above. This work runs on the operation initialization thread pool.

For live (refreshing) tables, Deephaven also registers the result in the [update graph](../dag.md) during initialization so it receives future updates.

**Updates**: After initialization, a live table updates whenever its source data changes. Each update uses the same concurrent row and column calculations as initialization, and independent tables update concurrently. This work runs on the update graph thread pool.

Both pools use all CPU cores by default. See [Configuration](#configuration) to size them.

<!-- excerpt gap: text between these passages omitted -->

## Python formulas and free-threading

Most Python builds use the GIL (global interpreter lock), which prevents concurrent execution of Python code across threads. Deephaven only splits a Python-backed filter or formula across cores on a [free-threaded Python build](https://docs.python.org/3/howto/free-threading-python.html). On a standard (GIL-enabled) build, two different Python-backed columns in the same `update` can still run at the same time on different threads, so shared state is not safe there either. To get parallel execution of Python-backed formulas and filters, switch to a free-threaded Python build; no other Deephaven configuration is required.

Not being split across cores is not the same guarantee `with_serial` provides. The engine may still evaluate a non-parallelizable column out of row-set order. If your formula or filter has side effects that depend on row order, use `with_serial` regardless of which Python build you're running.

<!-- excerpt gap: text between these passages omitted -->

Use barriers when one operation must finish all its rows before another operation begins — for example, when column A populates a dictionary that column B reads from, when column A computes a running total that column B normalizes against, or when column A assigns sequential IDs that column B should continue from.

A [`Barrier`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier) creates an ordering dependency between two operations: one operation **declares** the barrier (it goes first), another **respects** it (it waits), and Deephaven guarantees the declaring operation completes all rows before the respecting operation begins. Each barrier can only be declared by one operation; multiple operations can respect the same barrier.

<!-- excerpt gap: text between these passages omitted -->

Column A gets values 0–9. Column B gets values 10–19. Without the barrier, both columns would race and produce unpredictable results. Without `with_serial`, rows within each column would also race.

> [!IMPORTANT]
> Barriers don't make a column execute serially. If your formula has shared mutable state, you typically need **both** `with_serial` (for sequential row processing within a column) **and** a barrier (for ordering between columns).

Both columns need `with_serial` here because both mutate the shared `counter`. That's not always true: if a respecting column only reads a value that the declaring column already finished writing (rather than mutating shared state itself), it doesn't need `with_serial` — the barrier alone guarantees the write happened first.

<!-- excerpt gap: text between these passages omitted -->

Execution order: A and B run in parallel (they don't depend on each other); D starts after A finishes (doesn't wait for B); C starts after both A and B finish.

Barriers work the same way for [`Filter`](../../reference/query-language/types/Filter.md) objects in `where` operations — use them when one filter has side effects that another depends on. This is uncommon; most filters are stateless and don't need barriers.

#### Implicit barriers

When implicit barriers are enabled, serial operations automatically create barriers between each other — two serial columns in the same `update` execute one after the other without explicit barriers:

- **Stateless mode (default)**: Serial operations only enforce row order within themselves, not between each other. Use explicit barriers if you need cross-operation ordering.
- **Stateful mode**: Serial operations automatically wait for each other. This is useful when operations share global state.

Most users don't need to change this setting — see [Configuration](#configuration).

## Configuration

Parallelization needs no configuration. These properties tune it; defaults and full descriptions are in [Query table configuration](../query-table-configuration.md).

<!-- excerpt gap: text between these passages omitted -->

The three mechanisms above apply to operations that compute and store their results when they run:

- Column calculations in [`update`](../../reference/table-operations/select/update.md) and [`select`](../../reference/table-operations/select/select.md).
- Filters in [`where`](../../reference/table-operations/filter/where.md) clauses.
- [`sort`](../../reference/table-operations/sort/sort.md), once the table is large enough to be worth splitting.

<!-- excerpt gap: text between these passages omitted -->

To fix this, create a `Selectable` object and apply `with_serial`:

```python order=result
from deephaven.table import Selectable
from deephaven import empty_table

counter = 0


def get_and_increment_counter() -> int:
    global counter
    ret = counter
    counter += 1
    return ret


# Force serial execution - rows processed one at a time, in order
col = Selectable.parse("ID = get_and_increment_counter()").with_serial()
result = empty_table(5_000_000).update(col)
```
