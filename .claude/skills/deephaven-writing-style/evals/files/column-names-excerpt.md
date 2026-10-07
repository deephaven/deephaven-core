---
title: Parallelization
---

<!-- Excerpt of docs/python/conceptual/query-engine/parallelization.md at 609b266973 (PR #7457), a Concept guide. Passages are verbatim; gaps are marked. -->

### Barriers

Use barriers when one operation must finish all its rows before another operation begins — for example, when column A populates a dictionary that column B reads from, when column A computes a running total that column B normalizes against, or when column A assigns sequential IDs that column B should continue from.

A [`Barrier`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier) creates an ordering dependency between two operations: one operation **declares** the barrier (it goes first), another **respects** it (it waits), and Deephaven guarantees the declaring operation completes all rows before the respecting operation begins. Each barrier can only be declared by one operation; multiple operations can respect the same barrier.

#### Example: extending the counter with a barrier

Building on the counter example above: consider two columns that share a counter, where column A should assign IDs 0–9 and column B should continue from 10–19. Without a barrier, both columns would start simultaneously, both read `counter = 0`, and produce overlapping, incorrect results. With a barrier, column A runs first (0–9), then column B starts where A left off (10–19):

<!-- excerpt gap: text between these passages omitted -->

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

Column A gets values 0–9. Column B gets values 10–19. Without the barrier, both columns would race and produce unpredictable results. Without `with_serial`, rows within each column would also race.

> [!IMPORTANT]
> Barriers don't make a column execute serially. If your formula has shared mutable state, you typically need **both** `with_serial` (for sequential row processing within a column) **and** a barrier (for ordering between columns).

Both columns need `with_serial` here because both mutate the shared `counter`. That's not always true: if a respecting column only reads a value that the declaring column already finished writing (rather than mutating shared state itself), it doesn't need `with_serial` — the barrier alone guarantees the write happened first.
