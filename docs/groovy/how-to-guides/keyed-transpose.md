---
title: Keyed transpose
---

This guide shows you how to use [`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md) to reshape a table from long format to wide format. In long format, each row holds one observation, such as one log entry with a `Date` and a `Level`. In wide format, each distinct value of a category column, such as each `Level`, becomes its own column.

## When to use a keyed transpose

Use [`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md) when you need to:

- **Pivot data from long to wide format**: Convert rows of categorical data into columns.
- **Create cross-tabulations**: Build summary tables with aggregated values.
- **Reshape time-series data**: Transform data where categories are in rows into a format where they become columns.
- **Prepare data for visualization**: Many charts require data in wide format.

## Basic usage

The simplest use case involves specifying:

1. A source table.
2. One or more aggregations to apply.
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
- Each unique `Level` value (`INFO`, `WARN`, `ERROR`) becomes a column.
- The `Count` aggregation counts the rows for each combination of `Date` and `Level`. A combination with no rows in the source, such as `2025-08-06` and `INFO`, is null.

## Multiple row keys

You can specify multiple row-by columns to create more granular groupings:

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

You can apply multiple aggregations at once. When you do, the operation prefixes each output column name with the aggregation's output column name:

```groovy order=result,source
import io.deephaven.engine.table.impl.util.KeyedTranspose

source = newTable(
    stringCol("Product", "Widget", "Widget", "Widget", "Gadget", "Gadget"),
    stringCol("Region", "North", "North", "South", "North", "South"),
    intCol("Sales", 100, 50, 150, 200, 175),
    doubleCol("Revenue", 1000.0, 600.0, 1500.0, 2000.0, 1750.0)
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

The resulting columns are `TotalSales_North`, `AvgRevenue_North`, `TotalSales_South`, and `AvgRevenue_South`. The operation groups the columns for each `Region` value together and orders them in the order you list the aggregations.

## Column naming

The [`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md) operation builds each output column name from the values in the `columnByColumns` columns. It first builds a base name from those values and the aggregations, as shown in the first three rows of the following table. It then cleans up the name by applying the remaining rules in order:

| Scenario                                                                | Column naming pattern                                         | Example                                         |
| ----------------------------------------------------------------------- | ------------------------------------------------------------- | ----------------------------------------------- |
| Single aggregation, single column-by                                    | Value from the column-by column                               | `INFO`, `WARN`                                  |
| Multiple column-by columns                                              | Values joined with underscores                                | `INFO_10`, `WARN_20`                            |
| Two or more aggregations                                                | Aggregation output column name, an underscore, then the value | `Count_INFO`, `Sum_WARN`                        |
| Reserved word (Java keyword or literal, or `i`, `ii`, `k`, `in`, `not`) | Prefixed with `column_`                                       | `class` → `column_class`                        |
| Invalid characters                                                      | Characters removed                                            | `Type-A` → `TypeA`                              |
| Starts with a number                                                    | Prefixed with `column_`, after invalid characters are removed | `123` → `column_123`, `1-2.3/4` → `column_1234` |
| Duplicate names                                                         | Numeric suffix added to every name after the first            | `INFO`, `INFO2`                                 |

The operation adds the aggregation-name prefix only when the list contains two or more aggregations. A single aggregation with several output columns, such as [`AggSum("Sales", "Revenue")`](../reference/table-operations/group-and-aggregate/AggSum.md), gets no prefix, so its names collide and receive numeric suffixes (`North`, `North2`). To get prefixed names, pass one aggregation per column, such as `List.of(AggSum("Sales"), AggSum("Revenue"))`.

This example applies three combinations of aggregations and column-by columns to the same source:

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

// Combined example showing all scenarios together
result = scenario1.naturalJoin(scenario2, "RowKey").naturalJoin(scenario3, "RowKey")
```

In this example:

- `Normal`: The value is a valid column name, so the operation keeps it unchanged.
- `column_1234`: The operation removes the invalid characters (`-`, `.`, `/`). The result starts with a number, so the operation adds the `column_` prefix.
- `column_123`: The value starts with a number, so the operation adds the `column_` prefix.
- `Sum_Normal`, `Count_Normal`: With multiple aggregations, the operation prefixes each name with the aggregation's output column name.
- `Sum_123`, `Count_123`: The operation adds the aggregation prefix before it cleans up the name, so the name starts with a letter and doesn't get the `column_` prefix.
- `INFO_10`, `WARN_10`: The operation joins the values of the two column-by columns with an underscore.

The operation aggregates the two `INFO` rows for each `RowKey` into one group before it transposes, so they produce one `INFO` column, not two.

### Duplicate names and reserved words

Duplicate names occur when different column-by values produce the same output name. In this example, `INFO` and `IN.FO` both become `INFO` after the `.` is removed. The first column keeps the name, and the second gets the suffix `2`. The value `class` is a reserved word (a Java keyword), so it gets the `column_` prefix:

```groovy order=result,source
import io.deephaven.engine.table.impl.util.KeyedTranspose

source = newTable(
    stringCol("RowKey", "A", "A", "A", "B", "B", "B"),
    stringCol("Category", "INFO", "IN.FO", "class", "INFO", "IN.FO", "class"),
    intCol("Value", 1, 2, 3, 4, 5, 6)
)

// Result columns: RowKey, INFO, INFO2, column_class
result = KeyedTranspose.keyedTranspose(
    source,
    List.of(AggSum("Value")),
    ColumnName.from("RowKey"),
    ColumnName.from("Category")
)
```

### Sanitize data before transposing

To control the column names yourself, clean the values before you call `keyedTranspose`. Without cleaning, `Type-A` becomes `TypeA`. This example replaces the hyphen with an underscore, so the columns are `Type_A` and `Type_B`:

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

## Ticking tables and initial groups

When the source is a [ticking table](../conceptual/table-types.md) (a table that updates as new data arrives), [`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md) fixes its output columns when the operation runs. The result still adds, removes, and updates rows, but it never adds columns. This has two consequences:

- If the source has no rows yet, `keyedTranspose` has no column-by values to build columns from, and it throws an exception.
- A column-by value that first appears after the operation runs has no column to go in. By default, the result fails with an error. See [Handle new column-by values](#handle-new-column-by-values).

To create the columns up front, pass the `initialGroups` parameter a table that lists every combination of row-by and column-by values that should appear in the result. The table must contain all of the row-by and column-by columns. Each distinct combination of row-by values in it creates a row in the result, even before the source has data for it.

In this example, the source starts empty and never contains an `ERROR` row, but the result has an `ERROR` column from the start. Because `initGroups` creates a group for every combination it lists, the `ERROR` cells hold a count of 0 rather than null:

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.util.KeyedTranspose

// One log entry per second, alternating between two nodes
source = timeTable("PT1s").update(
    "NodeId = ii % 2 == 0 ? 10 : 20",
    "Level = ii % 3 == 0 ? `WARN` : `INFO`"
)

// Every combination of NodeId and Level that should have a cell in the result
initGroups = newTable(
    intCol("NodeId", 10, 10, 10, 20, 20, 20),
    stringCol("Level", "INFO", "WARN", "ERROR", "INFO", "WARN", "ERROR")
)

result = KeyedTranspose.keyedTranspose(
    source,
    List.of(AggCount("Count")),
    ColumnName.from("NodeId"),
    ColumnName.from("Level"),
    initGroups
)
```

### Handle new column-by values

By default, a ticking result fails with an error if the source receives a column-by value that was in neither the source nor `initialGroups` when the operation ran. To ignore such values instead, use the six-argument overload and pass [`KeyedTranspose.NewColumnBehavior.IGNORE`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/util/KeyedTranspose.NewColumnBehavior.html). The result then has no column for the new value, and the rows that contain it don't contribute to any cell.

In this example, every fourth log entry has the level `DEBUG`, which is not in `initGroups`. With `IGNORE`, the result keeps the `INFO`, `WARN`, and `ERROR` columns and leaves out the `DEBUG` entries:

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.util.KeyedTranspose

source = timeTable("PT1s").update(
    "NodeId = ii % 2 == 0 ? 10 : 20",
    "Level = ii % 4 == 3 ? `DEBUG` : (ii % 3 == 0 ? `WARN` : `INFO`)"
)

initGroups = newTable(
    intCol("NodeId", 10, 10, 10, 20, 20, 20),
    stringCol("Level", "INFO", "WARN", "ERROR", "INFO", "WARN", "ERROR")
)

result = KeyedTranspose.keyedTranspose(
    source,
    List.of(AggCount("Count")),
    ColumnName.from("NodeId"),
    ColumnName.from("Level"),
    initGroups,
    KeyedTranspose.NewColumnBehavior.IGNORE
)
```

## Common patterns

The following examples apply a single aggregation and a single column-by column to common long-format datasets.

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

The aggregation doesn't have to be numeric. This example uses [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md) to place each text answer in its question's column:

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

- **Column count**: [`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md) creates one output column per unique combination of column-by values, times the number of aggregation output columns. Avoid high-cardinality column-by columns, which produce very wide tables.
- **Initial groups**: For a ticking source, use `initialGroups` to create every expected column when the operation runs.
- **New column-by values**: Pass `KeyedTranspose.NewColumnBehavior.IGNORE` if the source can receive column-by values that aren't in `initialGroups`.
- **Aggregation choice**: Choose aggregations that make sense for your data. Common choices include [`AggCount`](../reference/table-operations/group-and-aggregate/AggCount.md), [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md), [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md), [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md), and [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md).

## Related documentation

- [Multi-aggregation](./combined-aggregations.md)
- [Table types](../conceptual/table-types.md)
- [`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/util/KeyedTranspose.html)
