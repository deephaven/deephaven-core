---
title: Table operations overview
sidebar_label: Operations overview
---

<div className="comment-title">

A quick tour of table operations

</div>

This guide provides a practical overview of Deephaven's table operations — what's available and when to use each. For the underlying concepts (immutability, formulas, dependencies), see [Understanding the Table API](./table-api.md).

## Basic pattern

Every table operation returns a new table:

```groovy order=source,filtered,enriched
source = emptyTable(5).update("X = ii")
filtered = source.where("X > 2")
enriched = filtered.update("Y = X * 10")
```

**Key principles:**

- **Operations return new tables.** The source table is never modified. `filtered` is a new table; `source` remains unchanged.
- **Formulas are strings.** Expressions like `"X > 2"` and `"Y = X * 10"` are executed by Deephaven's Java engine.
- **Dependencies are tracked.** If `source` is a live table, `filtered` and `enriched` automatically update when `source` changes.
- **Chain operations freely.** Each operation returns a table, so you can chain them together.

## Operation categories

### Filtering rows

Use filtering operations to select which rows appear in your result.

#### `where` — filter by condition

The most common filter. Keep rows where a condition is true:

```groovy order=source,highValue
source = newTable(
    stringCol("Symbol", "AAPL", "GOOG", "MSFT", "AAPL", "GOOG"),
    doubleCol("Price", 150.0, 2800.0, 300.0, 155.0, 2750.0)
)

highValue = source.where("Price > 500")
```

Multiple conditions in a single `where` are combined with AND:

```groovy order=source,result
source = newTable(
    stringCol("Symbol", "AAPL", "GOOG", "MSFT", "AAPL", "GOOG"),
    doubleCol("Price", 150.0, 2800.0, 300.0, 155.0, 2750.0)
)

result = source.where("Symbol = `AAPL`", "Price > 152")
```

#### `head`, `tail`, `slice` — filter by position

Select rows by their position in the table:

```groovy order=source,firstThree,lastTwo
source = emptyTable(10).update("X = ii")
firstThree = source.head(3)
lastTwo = source.tail(2)
```

### Transforming columns

These operations add, modify, or select columns.

#### `update` — add or replace columns (stored)

Creates new columns and stores results in memory. Use when:

- You'll access the column values multiple times
- The formula is expensive to compute

```groovy order=source,result
source = newTable(
    stringCol("Symbol", "AAPL", "GOOG", "MSFT"),
    doubleCol("Price", 150.0, 2800.0, 300.0),
    intCol("Quantity", 100, 50, 200)
)

result = source.update("Total = Price * Quantity", "PriceRounded = Math.round(Price)")
```

#### `view` — add or replace columns (computed on demand)

Like `update`, but computes values on demand instead of storing them. Use when:

- The formula is simple
- Memory is constrained
- You only need the values occasionally

```groovy order=source,result
source = newTable(doubleCol("X", 1.0, 2.0, 3.0, 4.0, 5.0))

result = source.view("X", "Doubled = X * 2", "Squared = X * X")
```

#### `select` — choose specific columns

Returns a table with only the specified columns. New columns can be defined inline:

```groovy order=source,result
source = newTable(
    stringCol("Symbol", "AAPL", "GOOG", "MSFT"),
    doubleCol("Price", 150.0, 2800.0, 300.0),
    intCol("Quantity", 100, 50, 200)
)

result = source.select("Symbol", "Total = Price * Quantity")
```

#### When to use each

| Operation | Stores results | Includes source columns | Best for                              |
| --------- | -------------- | ----------------------- | ------------------------------------- |
| `update`  | Yes            | Yes                     | Expensive formulas, repeated access   |
| `view`    | No             | Yes                     | Simple formulas, memory efficiency    |
| `select`  | Yes            | Only specified          | Reducing columns, creating new tables |

For the full comparison — including `lazyUpdate` and the memory/computation tradeoffs behind each choice — see [Memory vs computation tradeoffs](./table-api.md#memory-vs-computation-tradeoffs).

### Sorting

Order rows by column values:

```groovy order=source,byPrice,descending
source = newTable(
    stringCol("Symbol", "AAPL", "GOOG", "MSFT"),
    doubleCol("Price", 150.0, 2800.0, 300.0)
)

byPrice = source.sort("Price")
descending = source.sortDescending("Price")
```

### Joining tables

Joins combine columns from two tables based on matching keys. Deephaven offers several join types:

| Join              | Use case                                         |
| ----------------- | ------------------------------------------------ |
| `naturalJoin`     | Add columns from a lookup table (1:1 or many:1)  |
| `exactJoin`       | Like naturalJoin, but requires exactly one match |
| `join`            | Cross join with optional key matching            |
| `aj` (as-of join) | Match on a timestamp or ordered key              |

```groovy order=trades,symbols,enriched
trades = newTable(
    stringCol("Symbol", "AAPL", "GOOG", "MSFT"),
    doubleCol("Price", 150.0, 2800.0, 300.0)
)

symbols = newTable(
    stringCol("Symbol", "AAPL", "GOOG", "MSFT"),
    stringCol("Name", "Apple Inc.", "Alphabet Inc.", "Microsoft Corp.")
)

enriched = trades.naturalJoin(symbols, "Symbol", "Name")
```

For detailed coverage of join semantics and examples, see [Exact and relational joins](../how-to-guides/joins-exact-relational.md) and [Time-series and range joins](../how-to-guides/joins-timeseries-range.md).

### Aggregating data

Aggregations summarize data by computing statistics over groups of rows.

```groovy order=source,summary
import static io.deephaven.api.agg.Aggregation.AggSum
import static io.deephaven.api.agg.Aggregation.AggAvg
import static io.deephaven.api.agg.Aggregation.AggCount

source = newTable(
    stringCol("Symbol", "AAPL", "AAPL", "GOOG", "GOOG", "MSFT"),
    doubleCol("Price", 150.0, 155.0, 2800.0, 2750.0, 300.0),
    intCol("Quantity", 100, 150, 50, 75, 200)
)

summary = source.aggBy([
    AggSum("TotalQty = Quantity"),
    AggAvg("AvgPrice = Price"),
    AggCount("TradeCount")
], "Symbol")
```

For dedicated aggregators (`sumBy`, `avgBy`, `countBy`, etc.) and advanced aggregation patterns, see [How to use dedicated aggregations](../how-to-guides/dedicated-aggregations.md).

## Chaining operations

Build complex transformations by chaining operations. Each step produces a table you can inspect:

```groovy order=trades,cleaned,enriched,summary
import static io.deephaven.api.agg.Aggregation.AggSum
import static io.deephaven.api.agg.Aggregation.AggAvg

// Start with source data
trades = newTable(
    stringCol("Symbol", "AAPL", "AAPL", "GOOG", "GOOG", "MSFT", "MSFT"),
    doubleCol("Price", 150.0, 155.0, 2800.0, 2750.0, 300.0, 0.0),
    intCol("Quantity", 100, 150, 50, 75, 200, 10)
)

// Filter invalid rows
cleaned = trades.where("Price > 0")

// Add computed columns
enriched = cleaned.update("Total = Price * Quantity")

// Summarize by symbol
summary = enriched.aggBy([
    AggSum("TotalValue = Total"),
    AggAvg("AvgPrice = Price")
], "Symbol")
```

Each intermediate table (`cleaned`, `enriched`) is available for debugging or reuse elsewhere.

## Quick reference

| I want to...                     | Use this                                |
| -------------------------------- | --------------------------------------- |
| Keep rows matching a condition   | `where("condition")`                    |
| Get first/last N rows            | `head(n)` / `tail(n)`                   |
| Add columns (stored)             | `update("NewCol = formula")`            |
| Add columns (computed on demand) | `view("NewCol = formula")`              |
| Keep only specific columns       | `select("Col1", "Col2")`                |
| Sort by column                   | `sort("Column")`                        |
| Join lookup data                 | `naturalJoin(other, "Key", "AddedCol")` |
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
- [Exact and relational joins](../how-to-guides/joins-exact-relational.md)
- [Time-series and range joins](../how-to-guides/joins-timeseries-range.md)
- [How to use dedicated aggregations](../how-to-guides/dedicated-aggregations.md)
