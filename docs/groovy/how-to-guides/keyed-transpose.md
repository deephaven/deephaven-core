---
title: Keyed transpose
---

This guide shows you how to use [`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md) to reshape a table from long format to wide format. In long format, each row holds one observation, such as one log entry with a `Date` and a `Level`. In wide format, each distinct value of a category column, such as each `Level`, becomes its own column. This works like a pivot table, flattened into a single Deephaven table that you can use in further table operations like any other.

[`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md) works on real-time data as well as static data. When the source is a [ticking table](../conceptual/table-types.md), the result keeps updating as new rows arrive, but its set of columns is fixed when the operation runs. [Ticking tables and initial groups](#ticking-tables-and-initial-groups) shows how to plan for that.

## When to use a keyed transpose

Use [`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md) when you need to:

- **Pivot data from long to wide format**: Convert rows of categorical data into columns.
- **Create cross-tabulations**: Build summary tables with aggregated values.
- **Compare series side by side**: Give each metric, sensor, or symbol its own column, with one row per timestamp.
- **Prepare data for visualization**: Produce the one-column-per-series wide format that some charts expect.

## Basic usage

The simplest use case involves specifying:

1. A source table.
2. One or more [aggregations](./combined-aggregations.md) to apply.
3. Columns whose values identify each output row (`rowByColumns`). This guide calls these the _row-by columns_.
4. Columns whose values become new column names (`columnByColumns`). This guide calls these the _column-by columns_.

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
- The operation groups the source rows by each combination of `Date` and `Level`. Each combination is a _group_. The operation applies the [`AggCount`](../reference/table-operations/group-and-aggregate/AggCount.md) [aggregation](./combined-aggregations.md) to each group and puts the count in that group's cell.
- A combination with no rows in the source, such as `2025-08-06` and `INFO`, has no group, so its cell is null.

## Multiple row-by columns

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
    List.of(AggSum("TotalCount = Count")),
    ColumnName.from("Date", "Server"),
    ColumnName.from("Level")
)
```

Each unique combination of `Date` and `Server` creates a separate row in the output.

## Multiple column-by columns

You can also specify multiple column-by columns. The operation creates one output column for each unique combination of their values and joins the values with an underscore:

```groovy order=result,source
import io.deephaven.engine.table.impl.util.KeyedTranspose

source = newTable(
    stringCol("Date", "2025-08-05", "2025-08-05", "2025-08-06", "2025-08-06"),
    stringCol("Level", "INFO", "WARN", "INFO", "INFO"),
    intCol("NodeId", 10, 10, 10, 20)
)

result = KeyedTranspose.keyedTranspose(
    source,
    List.of(AggCount("Count")),
    ColumnName.from("Date"),
    ColumnName.from("Level", "NodeId")
)
```

The result has one column for each combination of `Level` and `NodeId` in the source: `INFO_10`, `WARN_10`, and `INFO_20`. For how these names interact with the other naming rules, see [Column naming](#column-naming).

## Multiple aggregations

You can apply multiple [aggregations](./combined-aggregations.md) at once. When you do, the operation prefixes each result column name with the output column name of the [aggregation](./combined-aggregations.md) that produced it:

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
        AggSum("TotalSales = Sales"),
        AggAvg("AvgRevenue = Revenue")
    ),
    ColumnName.from("Product"),
    ColumnName.from("Region")
)
```

The resulting columns are `TotalSales_North`, `AvgRevenue_North`, `TotalSales_South`, and `AvgRevenue_South`. The operation groups the columns for each `Region` value together. Within each group, the columns follow the order in which you list the [aggregations](./combined-aggregations.md).

The operation adds this prefix only when you pass two or more [aggregations](./combined-aggregations.md). It counts [aggregations](./combined-aggregations.md), not output columns:

- A single [`AggSum("Sales", "Revenue")`](../reference/table-operations/group-and-aggregate/AggSum.md) gets no prefix. Its names collide, so they get numeric suffixes, such as `North` and `North2` (see [Duplicate names and reserved words](#duplicate-names-and-reserved-words)).
- Two [aggregations](./combined-aggregations.md), `List.of(AggSum("Sales"), AggSum("Revenue"))`, get prefixed names, such as `Sales_North` and `Revenue_North`.

## Column naming

The [`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md) operation builds each output column name in two steps. First, it builds a base name from the column-by values and the [aggregations](./combined-aggregations.md):

| Scenario                                                                  | Base name                                                                                   | Example                  |
| ------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | ------------------------ |
| Single [aggregation](./combined-aggregations.md), single column-by column | Value from the column-by column                                                             | `INFO`, `WARN`           |
| Multiple column-by columns                                                | Values joined with underscores                                                              | `INFO_10`, `WARN_20`     |
| Two or more [aggregations](./combined-aggregations.md)                    | [Aggregation](./combined-aggregations.md) output column name, an underscore, then the value | `Count_INFO`, `Sum_WARN` |

Then it cleans up the base name by applying these rules, in this order:

1. **Reserved words**: A Java keyword or literal, or `i`, `ii`, `k`, `in`, or `not`, gets the prefix `column_`. For example, `class` becomes `column_class`.
2. **Invalid characters**: The operation removes every character other than ASCII letters, digits, `_`, and `$`. For example, `Type-A` becomes `TypeA`.
3. **Leading digits**: A name that starts with a digit after step 2 gets the prefix `column_`. For example, `123` becomes `column_123`, and `1-2.3/4` becomes `column_1234`.
4. **Duplicate names**: A name that repeats an earlier one gets a numeric suffix, starting at 2. For example, a second `INFO` becomes `INFO2`.

With a single [aggregation](./combined-aggregations.md) and a single column-by column, two kinds of value need care:

- A null column-by value becomes the reserved word `null`, so its column is named `column_null`.
- A value with no valid characters, such as an empty string or `--`, can't become a column name, so the operation fails.

This example applies three combinations of [aggregations](./combined-aggregations.md) and column-by columns to the same source. Each result appears in its own tab:

```groovy order=scenario1,scenario2,scenario3,source
import io.deephaven.engine.table.impl.util.KeyedTranspose

// Create a source table with various edge cases
source = newTable(
    stringCol("RowKey", "A", "A", "A", "A", "A", "A", "B", "B", "B", "B", "B", "B"),
    stringCol("Category", "Normal", "1-2.3/4", "123", "INFO", "INFO", "WARN", "Normal", "1-2.3/4", "123", "INFO", "INFO", "WARN"),
    intCol("NodeId", 1, 1, 1, 10, 10, 10, 1, 1, 1, 20, 20, 20),
    intCol("Value", 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60)
)

// Scenario 1: Single aggregation, single column-by column
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
        AggSum("Sum = Value"),
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
```

In this example:

- `Normal`: The value is a valid column name, so the operation keeps it unchanged.
- `column_1234`: The operation removes the invalid characters (`-`, `.`, `/`). The result starts with a number, so the operation adds the `column_` prefix.
- `column_123`: The value starts with a number, so the operation adds the `column_` prefix.
- `Sum_Normal`, `Count_Normal`: The two [aggregations](./combined-aggregations.md) add their output column names as prefixes (see [Multiple aggregations](#multiple-aggregations)).
- `Sum_123`, `Count_123`: The operation adds the [aggregation](./combined-aggregations.md) prefix before it cleans up the name, so the name starts with a letter and doesn't get the `column_` prefix.
- `INFO_10`, `WARN_10`: The operation joins the values of the two column-by columns with an underscore.

### Duplicate names and reserved words

Duplicate names occur when different column-by values produce the same output name, or when a single [aggregation](./combined-aggregations.md) produces several output columns (see [Multiple aggregations](#multiple-aggregations)).

Repeated values don't cause duplicate names. In the three-scenario example under [Column naming](#column-naming), the operation groups the two `INFO` rows for each `RowKey` together, so they produce one `INFO` column.

The following example shows a duplicate name and a reserved word:

- `INFO` and `IN.FO` both become `INFO` after the operation removes the `.`. The first column keeps the name, and the second gets the suffix `2`.
- The value `class` is a Java keyword, so it is a reserved word and gets the `column_` prefix.

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

A generated name that matches a row-by column name doesn't get a suffix. Instead, it causes an error. To avoid the error, rename the row-by column with [`renameColumns`](../reference/table-operations/select/rename-columns.md) or [sanitize the values](#sanitize-data-before-transposing) first.

### Sanitize data before transposing

To control the column names yourself, clean the values before you call [`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md). Without cleaning, `Type-A` becomes `TypeA`. This example replaces the hyphen with an underscore, so the columns are `Type_A` and `Type_B`:

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
    List.of(AggSum("Total = Value")),
    ColumnName.from("Date"),
    ColumnName.from("CleanCategory")
)
```

## Ticking tables and initial groups

When the source is a [ticking table](../conceptual/table-types.md), a table that updates as new data arrives, [`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md) sets its output columns once, at the moment the operation runs. The result still adds, removes, and updates rows, but it never adds columns. This has two consequences:

- If the source has no rows when the operation runs and you don't pass an `initialGroups` table with rows, [`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md) has no column-by values to build columns from, and it throws an exception.
- A column-by value that first appears after the operation runs has no column to go in. By default, the result fails with an error. See [Handle new column-by values](#handle-new-column-by-values).

To create the columns up front, pass the `initialGroups` parameter a table that lists every combination of row-by and column-by values that should appear in the result. The table must contain all of the row-by and column-by columns. Each distinct combination of row-by values in it creates a row in the result, even before the source has data for it.

In this example, the source starts empty and never contains an `ERROR` row, but the result has an `ERROR` column from the start. Because the table passed as `initialGroups` creates a group for every combination it lists, the `ERROR` cells hold a count of 0 rather than null:

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

The time table adds a row every second, so the result changes as you watch it. To see a result that stays put, let's swap the time table for an [input table](./input-tables.md), a table that changes only when you add rows to it. The input table starts empty, and the code then adds four rows, none of them `ERROR`:

```groovy order=result,source
import io.deephaven.engine.table.impl.util.KeyedTranspose
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.util.input.InputTableUpdater

source = AppendOnlyArrayBackedInputTable.make(
    TableDefinition.of(ColumnDefinition.ofInt("NodeId"), ColumnDefinition.ofString("Level"))
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
    initGroups
)

updater = (InputTableUpdater) source.getAttribute(Table.INPUT_TABLE_ATTRIBUTE)
updater.add(newTable(
    intCol("NodeId", 10, 10, 20, 20),
    stringCol("Level", "INFO", "WARN", "INFO", "INFO")
))
```

The result has one row for each `NodeId` and a column for each `Level` in `initGroups`. The `ERROR` column holds 0 for both nodes, and so does the `WARN` cell for node `20`.

### Handle new column-by values

By default, a [ticking](../conceptual/table-types.md) result fails with an error when the source receives a column-by value that has no column. This happens when:

- A value was in neither the source nor `initialGroups` when the operation ran.
- With several column-by columns, a combination of values is new.
- Without `initialGroups`, a value comes back after all of its rows were removed from the source.

To ignore such values instead, use the six-argument overload and pass [`KeyedTranspose.NewColumnBehavior.IGNORE`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/util/KeyedTranspose.NewColumnBehavior.html). The result then has no column for the new value, and the rows that contain it don't contribute to any cell.

In this example, every fourth log entry has the level `DEBUG`, which is not in the `initialGroups` table. With [`IGNORE`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/util/KeyedTranspose.NewColumnBehavior.html), the result keeps the `INFO`, `WARN`, and `ERROR` columns and leaves out the `DEBUG` entries:

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

To see the effect without waiting for ticks, this version uses an [input table](./input-tables.md) instead. Two of the four rows it adds have the level `DEBUG`:

```groovy order=result,source
import io.deephaven.engine.table.impl.util.KeyedTranspose
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.util.input.InputTableUpdater

source = AppendOnlyArrayBackedInputTable.make(
    TableDefinition.of(ColumnDefinition.ofInt("NodeId"), ColumnDefinition.ofString("Level"))
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

updater = (InputTableUpdater) source.getAttribute(Table.INPUT_TABLE_ATTRIBUTE)
updater.add(newTable(
    intCol("NodeId", 10, 10, 20, 20),
    stringCol("Level", "INFO", "DEBUG", "WARN", "DEBUG")
))
```

The `source` table shows all four rows, but the result has only the `INFO`, `WARN`, and `ERROR` columns. Each node has a count of 1 in one column and 0 in the others, so neither `DEBUG` row is counted.

## Common patterns

The following examples apply a single [aggregation](./combined-aggregations.md) and a single column-by column to common long-format datasets.

### Time-series metrics

This example turns one row per metric reading into one row per `Timestamp`, with a column for each `Metric`. Each timestamp has one reading per metric, so [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md) carries that reading into its cell:

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

The [aggregation](./combined-aggregations.md) doesn't have to be numeric. This example uses [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md) to place each text answer in its question's column:

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

- **Column count**: [`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md) creates one output column per unique combination of column-by values, times the number of [aggregation](./combined-aggregations.md) output columns. Avoid high-cardinality column-by columns, which produce very wide tables.
- **Initial groups**: For a [ticking](../conceptual/table-types.md) source, use `initialGroups` to create every expected column when the operation runs. See [Ticking tables and initial groups](#ticking-tables-and-initial-groups).
- **New column-by values**: Pass [`KeyedTranspose.NewColumnBehavior.IGNORE`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/util/KeyedTranspose.NewColumnBehavior.html) if the source can receive column-by values that aren't in `initialGroups`. See [Handle new column-by values](#handle-new-column-by-values).
- **[Aggregation](./combined-aggregations.md) choice**: Use [`AggCount`](../reference/table-operations/group-and-aggregate/AggCount.md) to count rows, [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md) or [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md) to combine numeric values, and [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md) or [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md) to carry one value, including text, into each cell.

## Related documentation

- [`keyedTranspose`](../reference/table-operations/format/keyedTranspose.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/util/KeyedTranspose.html)
- [Multi-aggregation](./combined-aggregations.md)
- [`AggCount`](../reference/table-operations/group-and-aggregate/AggCount.md)
- [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md)
- [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md)
- [`AggFirst`](../reference/table-operations/group-and-aggregate/AggFirst.md)
- [`AggLast`](../reference/table-operations/group-and-aggregate/AggLast.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`renameColumns`](../reference/table-operations/select/rename-columns.md)
- [Input tables](./input-tables.md)
- [Table types](../conceptual/table-types.md)
