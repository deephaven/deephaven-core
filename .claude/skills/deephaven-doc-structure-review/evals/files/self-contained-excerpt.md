---
title: Parallelization
---

<!-- Excerpt of docs/python/conceptual/query-engine/parallelization.md at 609b266973 (PR #7457), a Concept guide. Passage is verbatim. -->

Notice the duplicate value (two rows got `1`) and the out-of-order values (`4` before `3`).

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

When a `Selectable` is serial, every row is evaluated in order (row 0, then row 1, then row 2, etc.), and the column never runs concurrently with itself. That protects state that only this column uses. If another column uses the same state, add a [barrier](#barriers) as well. Barriers only order columns and filters within one operation, so if another table's formulas use the same state, make the shared code itself thread-safe (for example, protect it with a lock).

#### Serial filters

The same applies to filters. Deephaven parallelizes string-based filters in [`where`](../../reference/table-operations/filter/where.md) by default, so construct `Filter` objects explicitly when a filter has stateful side effects:

```python order=result
from deephaven import empty_table
from deephaven.filters import is_null, not_

# Create filters with serial evaluation
filter1 = is_null("X").with_serial()
filter2 = not_(is_null("Y")).with_serial()

result = (
    empty_table(1000)
    .update(["X = i % 5 == 0 ? null : i", "Y = i % 7 == 0 ? null : i"])
    .where([filter1, filter2])
)
```

When a [`Filter`](../../reference/query-language/types/Filter.md) is serial, every input row is evaluated in order, the filter cannot be reordered with respect to other filters, and stateful side effects happen sequentially. On tables from partitioned sources, marking a filter on partitioning columns serial also stops Deephaven from applying it to whole partitions before reading data — see [Filters on partitioning columns](../../reference/table-operations/filter/where.md#filters-on-partitioning-columns).

### Barriers

Use barriers when one operation must finish all its rows before another operation begins — for example, when column A populates a dictionary that column B reads from, when column A computes a running total that column B normalizes against, or when column A assigns sequential IDs that column B should continue from.

A [`Barrier`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier) creates an ordering dependency between two operations: one operation **declares** the barrier (it goes first), another **respects** it (it waits), and Deephaven guarantees the declaring operation completes all rows before the respecting operation begins. Each barrier can only be declared by one operation; multiple operations can respect the same barrier.

#### Example: extending the counter with a barrier

Building on the counter example above: consider two columns that share a counter, where column A should assign IDs 0–9 and column B should continue from 10–19. Without a barrier, both columns would start simultaneously, both read `counter = 0`, and produce overlapping, incorrect results. With a barrier, column A runs first (0–9), then column B starts where A left off (10–19):

