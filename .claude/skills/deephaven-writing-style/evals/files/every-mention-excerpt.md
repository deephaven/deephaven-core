---
title: update
---

<!-- Excerpt for a style-skill eval. Treat as docs/python/reference/table-operations/select/update.md, a Reference page. Related reference pages: ../../query-language/types/Selectable.md (with #with_serial), ./update-view.md, ../filter/where.md, ../../../how-to-guides/formulas.md, ../../../conceptual/query-engine/parallelization.md -->

The `update` method creates a new table with a new, in-memory column for each argument. Each argument is a [formula string](../../../how-to-guides/formulas.md) or a [`Selectable`](../../query-language/types/Selectable.md).

## Serial execution

By default, Deephaven parallelizes `update` calculations across CPU cores. A formula that has side effects can use [`with_serial`](../../query-language/types/Selectable.md#with_serial) to force sequential processing. When a column uses `with_serial`, Deephaven evaluates its rows one at a time, in row set order. A serial column that shares state with another column also needs a Barrier.

Related operations differ in how they handle a Selectable:

- `update_view` accepts only formula strings.
- `where` has its own Filter type with the same `with_serial` method.

| Method      | Accepts a Selectable | Serial columns    |
| ----------- | -------------------- | ----------------- |
| `update`    | Yes                  | `with_serial`     |
| `view`      | No                   | Not applicable    |

```python order=result
from deephaven import empty_table

result = empty_table(10).update(["X = i", "Y = X * 2"])
```

In the example, column `Y` depends on column `X`, and `update` evaluates both.
