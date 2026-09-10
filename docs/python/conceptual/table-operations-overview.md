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

```python order=source,filtered,enriched
from deephaven import empty_table

source = empty_table(5).update("X = ii")
filtered = source.where("X > 2")
enriched = filtered.update("Y = X * 10")
```

**Key principles:**

- **Operations return new tables.** The source table is never modified. `filtered` is a new table; `source` remains unchanged.
- **Formulas are strings.** Expressions like `"X > 2"` and `"Y = X * 10"` are executed by Deephaven's Java engine, not Python. Use Java syntax inside formula strings.
- **Dependencies are tracked.** If `source` is a live table, `filtered` and `enriched` automatically update when `source` changes.
- **Chain operations freely.** Each operation returns a table, so you can chain them together.

## Operation categories

### Filtering rows

Use filtering operations to select which rows appear in your result.

#### `where` — filter by condition

The most common filter. Keep rows where a condition is true:

```python order=source,high_value
from deephaven import new_table
from deephaven.column import string_col, double_col

source = new_table(
    [
        string_col("Symbol", ["AAPL", "GOOG", "MSFT", "AAPL", "GOOG"]),
        double_col("Price", [150.0, 2800.0, 300.0, 155.0, 2750.0]),
    ]
)

high_value = source.where("Price > 500")
```

Multiple conditions in a single `where` are combined with AND:

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, double_col

source = new_table(
    [
        string_col("Symbol", ["AAPL", "GOOG", "MSFT", "AAPL", "GOOG"]),
        double_col("Price", [150.0, 2800.0, 300.0, 155.0, 2750.0]),
    ]
)

result = source.where(["Symbol = `AAPL`", "Price > 152"])
```

#### `head`, `tail`, `slice` — filter by position

Select rows by their position in the table:

```python order=source,first_three,last_two
from deephaven import empty_table

source = empty_table(10).update("X = ii")
first_three = source.head(3)
last_two = source.tail(2)
```

### Transforming columns

These operations add, modify, or select columns.

#### `update` — add or replace columns (stored)

Creates new columns and stores results in memory. Use when:

- You'll access the column values multiple times
- The formula is expensive to compute

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, double_col, int_col

source = new_table(
    [
        string_col("Symbol", ["AAPL", "GOOG", "MSFT"]),
        double_col("Price", [150.0, 2800.0, 300.0]),
        int_col("Quantity", [100, 50, 200]),
    ]
)

result = source.update(["Total = Price * Quantity", "PriceRounded = Math.round(Price)"])
```

#### `view` — add or replace columns (computed on demand)

Like `update`, but computes values on demand instead of storing them. Use when:

- The formula is simple
- Memory is constrained
- You only need the values occasionally

```python order=source,result
from deephaven import new_table
from deephaven.column import double_col

source = new_table([double_col("X", [1.0, 2.0, 3.0, 4.0, 5.0])])

result = source.view(["X", "Doubled = X * 2", "Squared = X * X"])
```

#### `select` — choose specific columns

Returns a table with only the specified columns. New columns can be defined inline:

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, double_col, int_col

source = new_table(
    [
        string_col("Symbol", ["AAPL", "GOOG", "MSFT"]),
        double_col("Price", [150.0, 2800.0, 300.0]),
        int_col("Quantity", [100, 50, 200]),
    ]
)

result = source.select(["Symbol", "Total = Price * Quantity"])
```

#### When to use each

| Operation | Stores results | Includes source columns | Best for                              |
| --------- | -------------- | ----------------------- | ------------------------------------- |
| `update`  | Yes            | Yes                     | Expensive formulas, repeated access   |
| `view`    | No             | Yes                     | Simple formulas, memory efficiency    |
| `select`  | Yes            | Only specified          | Reducing columns, creating new tables |

For the full comparison — including `lazy_update` and the memory/computation tradeoffs behind each choice — see [Memory vs computation tradeoffs](./table-api.md#memory-vs-computation-tradeoffs).

### Sorting

Order rows by column values:

```python order=source,by_price,descending
from deephaven import new_table
from deephaven.column import string_col, double_col

source = new_table(
    [
        string_col("Symbol", ["AAPL", "GOOG", "MSFT"]),
        double_col("Price", [150.0, 2800.0, 300.0]),
    ]
)

by_price = source.sort("Price")
descending = source.sort_descending("Price")
```

### Joining tables

Joins combine columns from two tables based on matching keys. Deephaven offers several join types:

| Join              | Use case                                          |
| ----------------- | ------------------------------------------------- |
| `natural_join`    | Add columns from a lookup table (1:1 or many:1)   |
| `exact_join`      | Like natural_join, but requires exactly one match |
| `join`            | Cross join with optional key matching             |
| `aj` (as-of join) | Match on a timestamp or ordered key               |

```python order=trades,symbols,enriched
from deephaven import new_table
from deephaven.column import string_col, double_col

trades = new_table(
    [
        string_col("Symbol", ["AAPL", "GOOG", "MSFT"]),
        double_col("Price", [150.0, 2800.0, 300.0]),
    ]
)

symbols = new_table(
    [
        string_col("Symbol", ["AAPL", "GOOG", "MSFT"]),
        string_col("Name", ["Apple Inc.", "Alphabet Inc.", "Microsoft Corp."]),
    ]
)

enriched = trades.natural_join(symbols, on=["Symbol"], joins=["Name"])
```

For detailed coverage of join semantics and examples, see [Exact and relational joins](../how-to-guides/joins-exact-relational.md) and [Time-series and range joins](../how-to-guides/joins-timeseries-range.md).

### Aggregating data

Aggregations summarize data by computing statistics over groups of rows.

```python order=source,summary
from deephaven import new_table, agg
from deephaven.column import string_col, double_col, int_col

source = new_table(
    [
        string_col("Symbol", ["AAPL", "AAPL", "GOOG", "GOOG", "MSFT"]),
        double_col("Price", [150.0, 155.0, 2800.0, 2750.0, 300.0]),
        int_col("Quantity", [100, 150, 50, 75, 200]),
    ]
)

summary = source.agg_by(
    [
        agg.sum_("TotalQty = Quantity"),
        agg.avg("AvgPrice = Price"),
        agg.count_("TradeCount"),
    ],
    by=["Symbol"],
)
```

For dedicated aggregators (`sum_by`, `avg_by`, `count_by`, etc.) and advanced aggregation patterns, see [How to use dedicated aggregations](../how-to-guides/dedicated-aggregations.md).

## Chaining operations

Build complex transformations by chaining operations. Each step produces a table you can inspect:

```python order=trades,cleaned,enriched,summary
from deephaven import new_table, agg
from deephaven.column import string_col, double_col, int_col

# Start with source data
trades = new_table(
    [
        string_col("Symbol", ["AAPL", "AAPL", "GOOG", "GOOG", "MSFT", "MSFT"]),
        double_col("Price", [150.0, 155.0, 2800.0, 2750.0, 300.0, 0.0]),
        int_col("Quantity", [100, 150, 50, 75, 200, 10]),
    ]
)

# Filter invalid rows
cleaned = trades.where("Price > 0")

# Add computed columns
enriched = cleaned.update("Total = Price * Quantity")

# Summarize by symbol
summary = enriched.agg_by(
    [agg.sum_("TotalValue = Total"), agg.avg("AvgPrice = Price")], by=["Symbol"]
)
```

Each intermediate table (`cleaned`, `enriched`) is available for debugging or reuse elsewhere.

## Quick reference

| I want to...                     | Use this                              |
| -------------------------------- | ------------------------------------- |
| Keep rows matching a condition   | `where("condition")`                  |
| Get first/last N rows            | `head(n)` / `tail(n)`                 |
| Add columns (stored)             | `update(["NewCol = formula"])`        |
| Add columns (computed on demand) | `view(["NewCol = formula"])`          |
| Keep only specific columns       | `select(["Col1", "Col2"])`            |
| Sort by column                   | `sort("Column")`                      |
| Join lookup data                 | `natural_join(other, on=["Key"])`     |
| Aggregate by groups              | `agg_by([agg.sum_(...)], by=["Key"])` |

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
- [Exact and relational joins](../how-to-guides/joins-exact-relational.md)
- [Time-series and range joins](../how-to-guides/joins-timeseries-range.md)
- [How to use dedicated aggregations](../how-to-guides/dedicated-aggregations.md)
