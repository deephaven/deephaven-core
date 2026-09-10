---
title: Table operations overview
sidebar_label: Operations overview
---

<div className="comment-title">

A quick reference for table operations

</div>

For the concepts behind table operations — immutability, formulas, dependencies — see [Understanding the Table API](./table-api.md). This page is a quick-reference index of what's available; each guide linked below covers its operation in full, with examples.

## Quick reference

| I want to...                     | Use this                                |
| -------------------------------- | --------------------------------------- |
| Keep rows matching a condition   | `where("condition")`                    |
| Get first/last N rows            | `head(n)` / `tail(n)`                   |
| Add columns (stored)             | `update("NewCol = formula")`            |
| Add columns (computed on demand) | `view("NewCol = formula")`              |
| Keep only specific columns       | `select("Col1", "Col2")`                |
| Sort by column                   | `sort("Column")`                        |
| Sort in reverse order            | `sortDescending("Column")`              |
| Join lookup data                 | `naturalJoin(other, "Key", "AddedCol")` |
| Join requiring exactly one match | `exactJoin(other, "Key", "AddedCol")`   |
| Cross or key-matched join        | `join(other, "Key")`                    |
| Match on a timestamp/ordered key | `aj(other, "Key")`                      |
| Aggregate by groups              | `aggBy([AggSum(...)], "Key")`           |

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
