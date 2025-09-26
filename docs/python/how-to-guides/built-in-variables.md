---
title: Built-in query language variables
sidebar_label: Built-in Variables
---

There are three special built-in query language variables worth noting. They correspond to row indices in tables.

- `i` is a 32-bit integer representing the current row index.
- `ii` is a 64-bit integer representing the current row index.
- `k` is a 64-bit integer representing a special internal indexing value.

`i` and `ii` can be used to access the current, previous, and subsequent rows in a table.

> [!WARNING]
> `k` is a Deephaven engine index and does not correspond to traditional row indices. It is used for Deephaven engine development and should _only_ be used in limited circumstances, such as debugging or advanced query operations.

> [!WARNING]
> These built-in variables are not reliable in ticking tables. They should only be used in static cases.

## Usage

The following code block shows how to use `i` and `ii` in a query:

```python order=source
from deephaven import empty_table

source = empty_table(10).update(
    ["RowIndex32Bit = i", "RowIndex64Bit = ii", "EngineIndex64Bit = k"]
)
```

## Related documentation

- [Built-in constants](./built-in-constants.md)
- [Built-in functions](./built-in-functions.md)
- [Query string overview](./query-string-overview.md)
- [Filters](./filters.md)
- [Formulas](./formulas.md)
- [Work with arrays](./work-with-arrays.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`update`](../reference/table-operations/select/update.md)
