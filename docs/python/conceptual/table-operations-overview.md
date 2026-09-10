---
title: Table operations overview
sidebar_label: Operations overview
---

<div className="comment-title">

A quick reference for table operations

</div>

For the concepts behind table operations — immutability, formulas, dependencies — see [Understanding the Table API](./table-api.md). This page is a quick-reference index of what's available; each guide linked below covers its operation in full, with examples.

## Quick reference

| I want to...                                          | Use this                                                                                                              |
| ----------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| Keep rows matching a condition                        | [`where("condition")`](../reference/table-operations/filter/where.md)                                                 |
| Get first/last N rows                                 | [`head(n)`](../reference/table-operations/filter/head.md) / [`tail(n)`](../reference/table-operations/filter/tail.md) |
| Add columns (stored)                                  | [`update(["NewCol = formula"])`](../reference/table-operations/select/update.md)                                      |
| Add columns (computed on demand)                      | [`update_view(["NewCol = formula"])`](../reference/table-operations/select/update-view.md)                            |
| Keep only specific columns                            | [`select(["Col1", "Col2"])`](../reference/table-operations/select/select.md)                                          |
| Compute specific columns on demand, dropping the rest | [`view(["Col1", "NewCol = formula"])`](../reference/table-operations/select/view.md)                                  |
| Sort by column                                        | [`sort("Column")`](../reference/table-operations/sort/sort.md)                                                        |
| Sort in reverse order                                 | [`sort_descending("Column")`](../reference/table-operations/sort/sort-descending.md)                                  |
| Join lookup data                                      | [`natural_join(other, on=["Key"])`](../reference/table-operations/join/natural-join.md)                               |
| Join requiring exactly one match                      | [`exact_join(other, on=["Key"])`](../reference/table-operations/join/exact-join.md)                                   |
| Cross or key-matched join                             | [`join(other, on=["Key"])`](../reference/table-operations/join/join.md)                                               |
| Match on a timestamp/ordered key                      | [`aj(other, on=["Key"])`](../reference/table-operations/join/aj.md)                                                   |
| Aggregate by groups                                   | [`agg_by([agg.sum_(...)], by=["Key"])`](../reference/table-operations/group-and-aggregate/aggBy.md)                   |

## Diving deeper: API references

When you need complete method signatures, parameter details, or edge case behavior:

- **Pydoc** — [deephaven.table.Table](/core/pydoc/code/deephaven.table.html#deephaven.table.Table): All Python Table methods with type hints and docstrings
- **Javadoc** — [TableOperations](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html): The operation contracts that define behavior across all languages

The reference documentation for each operation (e.g., [`where`](../reference/table-operations/filter/where.md), [`update`](../reference/table-operations/select/update.md)) also links to the relevant Pydoc and Javadoc.

## Related documentation

- [Understanding the Table API](./table-api.md)
- [Deephaven's design](./deephaven-design.md)
- [Table types](./table-types.md)
- [How to use filters](../how-to-guides/use-filters.md)
- [How to use select, view, and update](../how-to-guides/use-select-view-update.md)
- [Sort table data](../how-to-guides/sort.md)
- [Exact and relational joins](../how-to-guides/joins-exact-relational.md)
- [Time-series and range joins](../how-to-guides/joins-timeseries-range.md)
- [How to use dedicated aggregations](../how-to-guides/dedicated-aggregations.md)
