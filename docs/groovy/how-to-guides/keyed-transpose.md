---
title: Keyed transpose
---

This guide shows you how to use [`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md) to transform data from a long format to a wide format by pivoting column values into new column names. This is useful when you need to reshape data for analysis, reporting, or visualization.

## When to use `keyed transpose`

Use [`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md) when you need to:

- **Pivot data from long to wide format**: Convert rows of categorical data into columns.
- **Create cross-tabulations**: Build summary tables with aggregated values.
- **Reshape time-series data**: Transform data where categories are in rows into a format where they become columns.
- **Prepare data for visualization**: Many charts require data in wide format.

## Basic usage

The simplest use case involves specifying:

1. A source table.
2. An aggregation to apply.
3. Columns to use as row keys (`rowByColumns`).
4. Columns whose values become new column names (`columnByColumns`).

```groovy order=result,source
import io.deephaven.engine.table.impl.util.KeyedTranspose

source = newTable(
    stringCol("Date", "2025-08-05", "2025-08-05", "2025-08-06", "2025-08-07"),
    stringCol("Level", "INFO", "INFO", "WARN", "ERROR")
)

result = KeyedTranspose.keyedTranspose(
    source, 
    List.of(AggCount("Count")),
    ColumnName.from("Date"), 
    ColumnName.from("Level")
)
```

In this example:

- Each unique `Date` becomes a row.
- Each unique `Level` value (INFO, WARN, ERROR) becomes a column.
- The `Count` aggregation counts occurrences for each Date-Level combination.

## Multiple row keys

You can specify multiple columns as row keys to create more granular groupings:

```groovy order=result,source
import io.deephaven.engine.table.impl.util.KeyedTranspose

source = newTable(
    stringCol("Date", "2025-08-05", "2025-08-05", "2025-08-06", "2025-08-06"),
    stringCol("Server", "Server1", "Server2", "Server1", "Server2"),
    stringCol("Level", "INFO", "WARN", "INFO", "ERROR"),
    intCol("Count", 10, 5, 15, 2)
)

result = KeyedTranspose.keyedTranspose(
    source, 
    List.of(AggSum("TotalCount=Count")),
    ColumnName.from("Date", "Server"),
    ColumnName.from("Level")
)
```

Each unique combination of `Date` and `Server` creates a separate row in the output.

## Multiple aggregations

You can apply multiple aggregations simultaneously. When you do this, column names are prefixed with the aggregation name:

```groovy order=result,source
import io.deephaven.engine.table.impl.util.KeyedTranspose

source = newTable(
    stringCol("Product", "Widget", "Widget", "Gadget", "Gadget"),
    stringCol("Region", "North", "South", "North", "South"),
    intCol("Sales", 100, 150, 200, 175),
    doubleCol("Revenue", 1000.0, 1500.0, 2000.0, 1750.0)
)

result = KeyedTranspose.keyedTranspose(
    source,
    List.of(
        AggSum("TotalSales=Sales"),
        AggAvg("AvgRevenue=Revenue")
    ),
    ColumnName.from("Product"),
    ColumnName.from("Region")
)
```

The resulting columns will be named like `TotalSales_North`, `TotalSales_South`, `AvgRevenue_North`, and `AvgRevenue_South`.

## Initial groups for ticking tables

When working with ticking (live updating) tables, you may want to ensure all expected columns exist from the start, even if no data has yet arrived. Use the `initialGroups` parameter:

```groovy order=result,source
import io.deephaven.engine.table.impl.util.KeyedTranspose

source = newTable(
    stringCol("Date", "2025-08-05", "2025-08-05"),
    stringCol("Level", "INFO", "INFO"),
    intCol("NodeId", 10, 10)
)

// Define all expected combinations of Level and NodeId
initGroups = newTable(
    stringCol("Level", "ERROR", "WARN", "INFO", "ERROR", "WARN", "INFO"),
    intCol("NodeId", 10, 10, 10, 20, 20, 20)
).join(source.selectDistinct("Date"))

result = KeyedTranspose.keyedTranspose(
    source, 
    List.of(AggCount("Count")), 
    ColumnName.from("Date"),
    ColumnName.from("Level", "NodeId"), 
    initGroups
)
```

Even though the source only has INFO logs from NodeId 10, the result will include columns for all Level-NodeId combinations specified in `initGroups`.

## Column naming

The `keyedTranspose` operation follows specific rules for naming output columns:

| Scenario                             | Column Naming Pattern         | Example                  |
| ------------------------------------ | ----------------------------- | ------------------------ |
| Single aggregation, single column-by | Value from column-by column   | `INFO`, `WARN`           |
| Multiple aggregations                | Aggregation name + value      | `Count_INFO`, `Sum_WARN` |
| Multiple column-by columns           | Values joined with underscore | `INFO_10`, `WARN_20`     |
| Invalid characters                   | Characters removed            | `Type-A` → `TypeA`       |
| Starts with number                   | Prefixed with `column_`       | `123` → `column_123`     |
| Duplicate names                      | Suffix added                  | `INFO`, `INFO2`          |

This example demonstrates each column naming scenario in the table above except duplicate names:

```groovy order=result,source
import io.deephaven.engine.table.impl.util.KeyedTranspose

// Create a source table with various edge cases
source = newTable(
    stringCol("RowKey", "A", "A", "A", "A", "A", "A", "B", "B", "B", "B", "B", "B"),
    stringCol("Category", "Normal", "1-2.3/4", "123", "INFO", "INFO", "WARN", "Normal", "1-2.3/4", "123", "INFO", "INFO", "WARN"),
    intCol("NodeId", 1, 1, 1, 10, 10, 10, 1, 1, 1, 20, 20, 20),
    intCol("Value", 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60)
)

// Scenario 1: Single aggregation, single column-by
// Result columns: RowKey, Normal, column_1234, column_123, INFO, WARN
scenario1 = KeyedTranspose.keyedTranspose(
    source,
    List.of(AggSum("Value")),
    ColumnName.from("RowKey"),
    ColumnName.from("Category")
)

// Scenario 2: Multiple aggregations
// Result columns: RowKey, Sum_Normal, Count_Normal, Sum_1234, Count_1234, Sum_123, Count_123, Sum_INFO, Count_INFO, Sum_WARN, Count_WARN
scenario2 = KeyedTranspose.keyedTranspose(
    source,
    List.of(
        AggSum("Sum=Value"),
        AggCount("Count")
    ),
    ColumnName.from("RowKey"),
    ColumnName.from("Category")
)

// Scenario 3: Multiple column-by columns
// Result columns: RowKey, Normal_1, column_1234_1, column_123_1, INFO_10, WARN_10, INFO_20, WARN_20
scenario3 = KeyedTranspose.keyedTranspose(
    source,
    List.of(AggSum("Value")),
    ColumnName.from("RowKey"),
    ColumnName.from("Category", "NodeId")
)

// Combine the three results into one table
result = scenario1.naturalJoin(scenario2, "RowKey").naturalJoin(scenario3, "RowKey")
```

In this example:

- **Normal**: Standard column name (single aggregation, single column-by).
- **column_1234**: Invalid characters (`-`, `.`, `/`) are removed, then `column_` is prefixed because the result starts with a number.
- **column_123**: Numeric value is prefixed with `column_`.
- **WARN**: Additional standard column name.
- **Sum_Normal**, **Count_Normal**: Multiple aggregations prefix the column name. The aggregation prefix is added before the name is cleaned up, so `123` becomes `Sum_123`. That name starts with a letter, so it doesn't get the `column_` prefix.
- **INFO_10**, **WARN_10**: Multiple column-by values are joined with underscores.

This example does not produce duplicate names. The two `INFO` rows for each `RowKey` are aggregated into a single group before transposing, so they produce one `INFO` column, not two. Duplicate names occur only when two different values clean up to the same name, such as `INFO` and `IN.FO`: the first becomes `INFO` and the second gets a numeric suffix, `INFO2`.

### Sanitize data before transposing

To maintain control over column names, clean your data values before using `keyedTranspose`:

```groovy order=result,source
import io.deephaven.engine.table.impl.util.KeyedTranspose

source = newTable(
    stringCol("Date", "2025-08-05", "2025-08-05", "2025-08-06"),
    stringCol("Category", "Type-A", "Type-B", "Type-A"),
    intCol("Value", 10, 20, 15)
)

// Sanitize category names to be more column-friendly
cleaned = source.update("CleanCategory = Category.replace('-', '_')")

result = KeyedTranspose.keyedTranspose(
    cleaned,
    List.of(AggSum("Total=Value")),
    ColumnName.from("Date"),
    ColumnName.from("CleanCategory")
)
```

## Simple examples

### Sales by region and product

```groovy order=salesData,result
import io.deephaven.engine.table.impl.util.KeyedTranspose

salesData = newTable(
    stringCol("Product", "Widget", "Widget", "Gadget", "Gadget", "Widget"),
    stringCol("Region", "North", "South", "North", "South", "East"),
    intCol("Sales", 100, 150, 200, 175, 125)
)

// Transform: Product | Region | Sales
// Into: Product | North | South | East
result = KeyedTranspose.keyedTranspose(
    salesData,
    List.of(AggSum("Sales")),
    ColumnName.from("Product"),
    ColumnName.from("Region")
)
```

### Time-series metrics

```groovy order=metricsData,result
import io.deephaven.engine.table.impl.util.KeyedTranspose

metricsData = newTable(
    stringCol("Timestamp", "10:00", "10:00", "10:01", "10:01", "10:02", "10:02"),
    stringCol("Metric", "CPU", "Memory", "CPU", "Memory", "CPU", "Memory"),
    doubleCol("Value", 45.2, 78.5, 52.1, 80.3, 48.7, 79.1)
)

// Transform: Timestamp | Metric | Value
// Into: Timestamp | CPU | Memory
result = KeyedTranspose.keyedTranspose(
    metricsData,
    List.of(AggLast("Value")),
    ColumnName.from("Timestamp"),
    ColumnName.from("Metric")
)
```

### Survey responses

```groovy order=surveyData,result
import io.deephaven.engine.table.impl.util.KeyedTranspose

surveyData = newTable(
    intCol("RespondentId", 1, 1, 1, 2, 2, 2, 3, 3, 3),
    stringCol("Question", "Q1", "Q2", "Q3", "Q1", "Q2", "Q3", "Q1", "Q2", "Q3"),
    stringCol("Answer", "Yes", "No", "Maybe", "No", "Yes", "Yes", "Yes", "Yes", "No")
)

// Transform: RespondentId | Question | Answer
// Into: RespondentId | Q1 | Q2 | Q3
result = KeyedTranspose.keyedTranspose(
    surveyData,
    List.of(AggFirst("Answer")),
    ColumnName.from("RespondentId"),
    ColumnName.from("Question")
)
```

## Best practices

- **Performance**: `keyedTranspose` creates new columns dynamically. This can create tables with many columns for very high-cardinality data (many unique values in `columnByColumns`).
- **Ticking tables**: Use `initialGroups` to ensure consistent column structure when working with live data.
  **Column limits**: Be mindful of the number of unique values in your `columnByColumns` — each becomes a separate column.
- **Aggregation choice**: Choose aggregations that make sense for your data. Common choices include [`AggCount`](../reference/table-operations/group-and-aggregate/AggCount.md), [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md), [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md), [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md), and [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md).

## Related documentation

- [Aggregations guide](./combined-aggregations.md)
- [`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/util/KeyedTranspose.html)
