---
title: Parallelization
---

<!-- Excerpt of docs/python/conceptual/query-engine/parallelization.md at 609b266973 (PR #7457), a Concept guide. Passages are verbatim except that the links on `Selectable` and `Filter` in the first two bullets of the opening section were removed, so the types are bare at first mention. Gaps are marked. -->

## Controlling execution order

Most queries work correctly with automatic parallelization. Some code doesn't — for example, code that uses a counter or modifies shared state. Deephaven provides two controls for these cases, `with_serial` and barriers, which you apply to these objects:

- **`Selectable`**: Represents a column expression, used in `select` or `update` operations.
- **`Filter`**: Represents a filter condition, used in `where` operations. Concurrency control works the same way for `Filter` as it does for `Selectable`.

**`with_serial` vs. barriers** — these solve different problems:

- **`with_serial`**: Rows _within one column_ are processed sequentially (row 0, then row 1, etc.). Other columns can still run at the same time.
- **Barriers**: _Between columns_, one column finishes all its rows before another column starts. Rows within each column can still be parallelized.

When shared state is involved, you often need both: `with_serial` to protect row-level access to the shared state, and a barrier to ensure one column is completely done before the other starts.

<!-- excerpt gap: text between these passages omitted -->

The [`ConcurrencyControl`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl) interface provides the [`with_serial`](../../reference/table-operations/select/update.md#serial-execution) method for [`Filter`](../../reference/query-language/types/Filter.md) ([`where`](../../reference/table-operations/filter/where.md#serial-execution)) and [`Selectable`](../../reference/query-language/types/Selectable.md) ([`update`](../../reference/table-operations/select/update.md#serial-execution) and [`select`](../../reference/table-operations/select/select.md)).

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
