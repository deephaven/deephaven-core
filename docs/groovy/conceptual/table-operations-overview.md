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
| Add columns (stored)                                  | [`update("NewColumn = formula")`](../reference/table-operations/select/update.md)                                     |
| Add columns (computed on demand)                      | [`updateView("NewColumn = formula")`](../reference/table-operations/select/update-view.md)                            |
| Keep only specific columns                            | [`select("Column1", "Column2")`](../reference/table-operations/select/select.md)                                      |
| Compute specific columns on demand, dropping the rest | [`view("Column1", "NewColumn = formula")`](../reference/table-operations/select/view.md)                              |
| Sort by column                                        | [`sort("Column")`](../reference/table-operations/sort/sort.md)                                                        |
| Sort in reverse order                                 | [`sortDescending("Column")`](../reference/table-operations/sort/sort-descending.md)                                   |
| Join lookup data                                      | [`naturalJoin(other, "Key", "AddedColumn")`](../reference/table-operations/join/natural-join.md)                      |
| Join requiring exactly one match                      | [`exactJoin(other, "Key", "AddedColumn")`](../reference/table-operations/join/exact-join.md)                          |
| Cross or key-matched join                             | [`join(other, "Key")`](../reference/table-operations/join/join.md)                                                    |
| Match on a timestamp/ordered key                      | [`aj(other, "Key")`](../reference/table-operations/join/aj.md)                                                        |
| Aggregate by groups                                   | [`aggBy([AggSum(...)], "Key")`](../reference/table-operations/group-and-aggregate/aggBy.md)                           |

## Diving deeper: API references

When you need complete method signatures, parameter details, or edge case behavior:

- **Javadoc** — [Table](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html): The Table interface with all methods
- **Javadoc** — [TableOperations](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html): The operation contracts that define behavior

The reference documentation for each operation (e.g., [`where`](../reference/table-operations/filter/where.md), [`update`](../reference/table-operations/select/update.md)) also links to the relevant Javadoc.

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
