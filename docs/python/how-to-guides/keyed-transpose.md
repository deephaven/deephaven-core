---
title: Keyed transpose
---

This guide shows you how to use [`keyed_transpose`](../reference/table-operations/format/keyed-transpose.md) to reshape a table from long format to wide format. In long format, each row holds one observation, such as one log entry with a `Date` and a `Level`. In wide format, each distinct value of a category column, such as each `Level`, becomes its own column.

## When to use a keyed transpose

Use [`keyed_transpose`](../reference/table-operations/format/keyed-transpose.md) when you need to:

- **Create cross-tabulations**: Build summary tables with one aggregated value per row and category.
- **Reshape time-series data**: Give each metric or series its own column, with one row per timestamp.
- **Prepare data for reports**: Build tables for reports or charts that expect one column per category.

## Basic usage

A keyed transpose takes four required arguments:

1. A source table.
2. One or more aggregations to apply.
3. The row-by columns (`row_by_cols`). Each unique combination of their values becomes a row in the result.
4. The column-by columns (`col_by_cols`). Each unique combination of their values becomes a column in the result.

Two optional arguments matter mainly for ticking sources: `initial_groups` creates result rows and columns before the source has data for them, and `new_column_behavior` controls what happens when a new column-by value arrives. `new_column_behavior` has no effect on a static source. See [Ticking tables and initial groups](#ticking-tables-and-initial-groups).

```python order=result,source
from deephaven import agg, new_table
from deephaven.column import string_col
from deephaven.table import keyed_transpose

source = new_table(
    [
        string_col("Date", ["2025-08-05", "2025-08-05", "2025-08-06", "2025-08-07"]),
        string_col("Level", ["INFO", "INFO", "WARN", "ERROR"]),
    ]
)

result = keyed_transpose(source, [agg.count_("Count")], ["Date"], ["Level"])
```

In this example:

- Each unique `Date` becomes a row.
- Each unique `Level` value (`INFO`, `WARN`, `ERROR`) becomes a column.
- The `Count` aggregation counts the rows for each combination of `Date` and `Level`. A combination with no rows in the source, such as `2025-08-06` and `INFO`, is null.

## Multiple row keys

You can specify multiple row-by columns to create more granular groupings:

```python order=result,source
from deephaven import agg, new_table
from deephaven.column import string_col, int_col
from deephaven.table import keyed_transpose

source = new_table(
    [
        string_col("Date", ["2025-08-05", "2025-08-05", "2025-08-06", "2025-08-06"]),
        string_col("Server", ["Server1", "Server2", "Server1", "Server2"]),
        string_col("Level", ["INFO", "WARN", "INFO", "ERROR"]),
        int_col("Count", [10, 5, 15, 2]),
    ]
)

result = keyed_transpose(
    source, [agg.sum_(["TotalCount=Count"])], ["Date", "Server"], ["Level"]
)
```

Each unique combination of `Date` and `Server` creates a separate row in the output.

## Multiple aggregations

You can apply multiple aggregations at once. When you do, the operation prefixes each output column name with the aggregation's output column name:

```python order=result,source
from deephaven import agg, new_table
from deephaven.column import string_col, int_col, double_col
from deephaven.table import keyed_transpose

source = new_table(
    [
        string_col("Product", ["Widget", "Widget", "Widget", "Gadget", "Gadget"]),
        string_col("Region", ["North", "North", "South", "North", "South"]),
        int_col("Sales", [100, 50, 150, 200, 175]),
        double_col("Revenue", [1000.0, 600.0, 1500.0, 2000.0, 1750.0]),
    ]
)

result = keyed_transpose(
    source,
    [agg.sum_(["TotalSales=Sales"]), agg.avg(["AvgRevenue=Revenue"])],
    ["Product"],
    ["Region"],
)
```

The resulting columns are `TotalSales_North`, `AvgRevenue_North`, `TotalSales_South`, and `AvgRevenue_South`. The operation groups the columns for each `Region` value together and orders them in the order you list the aggregations.

## Column naming

The [`keyed_transpose`](../reference/table-operations/format/keyed-transpose.md) operation builds each output column name from the values in the `col_by_cols` columns. It first builds a base name from those values and the aggregations, as shown in the first three rows of the following table. It then cleans up the name by applying the remaining rules in order:

| Rule                                                                    | Column naming pattern                                         | Example                                         |
| ----------------------------------------------------------------------- | ------------------------------------------------------------- | ----------------------------------------------- |
| Single aggregation, single column-by                                    | Value from the column-by column                               | `INFO`, `WARN`                                  |
| Multiple column-by columns                                              | Values joined with underscores                                | `INFO_10`, `WARN_20`                            |
| Two or more aggregations                                                | Aggregation output column name, an underscore, then the value | `Count_INFO`, `Sum_WARN`                        |
| Reserved word (Java keyword or literal, or `i`, `ii`, `k`, `in`, `not`) | Prefixed with `column_`                                       | `class` → `column_class`                        |
| Invalid characters                                                      | Characters removed                                            | `Type-A` → `TypeA`                              |
| Starts with a number                                                    | Prefixed with `column_`, after invalid characters are removed | `123` → `column_123`, `1-2.3/4` → `column_1234` |
| Duplicate names                                                         | Numeric suffix added to every name after the first            | `INFO`, `INFO2`                                 |

The operation adds the aggregation-name prefix only when you pass two or more aggregations. A single aggregation with several output columns, such as [`agg.sum_(["Sales", "Revenue"])`](../reference/table-operations/group-and-aggregate/AggSum.md), gets no prefix, so its names collide and receive numeric suffixes (`North`, `North2`). To get prefixed names, pass one aggregation per column, such as `[agg.sum_(["Sales"]), agg.sum_(["Revenue"])]`.

This example applies three combinations of aggregations and column-by columns to the same source:

```python order=result,source
from deephaven import agg, new_table
from deephaven.column import string_col, int_col
from deephaven.table import keyed_transpose

# Create a source table with various edge cases
source = new_table(
    [
        string_col(
            "RowKey", ["A", "A", "A", "A", "A", "A", "B", "B", "B", "B", "B", "B"]
        ),
        string_col(
            "Category",
            [
                "Normal",
                "1-2.3/4",
                "123",
                "INFO",
                "INFO",
                "WARN",
                "Normal",
                "1-2.3/4",
                "123",
                "INFO",
                "INFO",
                "WARN",
            ],
        ),
        int_col("NodeId", [1, 1, 1, 10, 10, 10, 1, 1, 1, 20, 20, 20]),
        int_col("Value", [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]),
    ]
)

# Scenario 1: Single aggregation, single column-by
# Result columns: RowKey, Normal, column_1234, column_123, INFO, WARN
scenario1 = keyed_transpose(source, [agg.sum_(["Value"])], ["RowKey"], ["Category"])

# Scenario 2: Multiple aggregations
# Result columns: RowKey, Sum_Normal, Count_Normal, Sum_1234, Count_1234, Sum_123, Count_123, Sum_INFO, Count_INFO, Sum_WARN, Count_WARN
scenario2 = keyed_transpose(
    source, [agg.sum_(["Sum=Value"]), agg.count_("Count")], ["RowKey"], ["Category"]
)

# Scenario 3: Multiple column-by columns
# Result columns: RowKey, Normal_1, column_1234_1, column_123_1, INFO_10, WARN_10, INFO_20, WARN_20
scenario3 = keyed_transpose(
    source, [agg.sum_(["Value"])], ["RowKey"], ["Category", "NodeId"]
)

# Combine the three results into one table
result = scenario1.natural_join(scenario2, ["RowKey"]).natural_join(
    scenario3, ["RowKey"]
)
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

```python order=result,source
from deephaven import agg, new_table
from deephaven.column import string_col, int_col
from deephaven.table import keyed_transpose

source = new_table(
    [
        string_col("RowKey", ["A", "A", "A", "B", "B", "B"]),
        string_col("Category", ["INFO", "IN.FO", "class", "INFO", "IN.FO", "class"]),
        int_col("Value", [1, 2, 3, 4, 5, 6]),
    ]
)

# Result columns: RowKey, INFO, INFO2, column_class
result = keyed_transpose(source, [agg.sum_(["Value"])], ["RowKey"], ["Category"])
```

### Sanitize data before transposing

To control the column names yourself, clean the values before you call `keyed_transpose`. Without cleaning, `Type-A` becomes `TypeA`. This example replaces the hyphen with an underscore, so the columns are `Type_A` and `Type_B`:

```python order=result,source
from deephaven import agg, new_table
from deephaven.column import string_col, int_col
from deephaven.table import keyed_transpose

source = new_table(
    [
        string_col("Date", ["2025-08-05", "2025-08-05", "2025-08-06"]),
        string_col("Category", ["Type-A", "Type-B", "Type-A"]),
        int_col("Value", [10, 20, 15]),
    ]
)

# Sanitize category names to be more column-friendly
cleaned = source.update(["CleanCategory = Category.replace('-', '_')"])

result = keyed_transpose(
    cleaned, [agg.sum_(["Total=Value"])], ["Date"], ["CleanCategory"]
)
```

## Ticking tables and initial groups

When the source is a [ticking table](../conceptual/table-types.md) (a table that updates as new data arrives), [`keyed_transpose`](../reference/table-operations/format/keyed-transpose.md) fixes its output columns when the operation runs. The result still adds, removes, and updates rows, but it never adds columns. This has two consequences:

- If the source has no rows yet, `keyed_transpose` has no column-by values to build columns from, and it raises an error.
- A column-by value that first appears after the operation runs has no column to go in. By default, the result fails with an error. See [Handle new column-by values](#handle-new-column-by-values).

To create the columns up front, pass the `initial_groups` parameter a table that lists every combination of row-by and column-by values that should appear in the result. The table must contain all of the row-by and column-by columns. Each distinct combination of row-by values in it creates a row in the result, even before the source has data for it.

In this example, the source starts empty and never contains an `ERROR` row, but the result has an `ERROR` column from the start. Because `init_groups` creates a group for every combination it lists, the `ERROR` cells hold a count of 0 rather than null:

```python ticking-table order=null
from deephaven import agg, new_table, time_table
from deephaven.column import int_col, string_col
from deephaven.table import keyed_transpose

# One log entry per second, alternating between two nodes
source = time_table("PT1s").update(
    ["NodeId = ii % 2 == 0 ? 10 : 20", "Level = ii % 3 == 0 ? `WARN` : `INFO`"]
)

# Every combination of NodeId and Level that should have a cell in the result
init_groups = new_table(
    [
        int_col("NodeId", [10, 10, 10, 20, 20, 20]),
        string_col("Level", ["INFO", "WARN", "ERROR", "INFO", "WARN", "ERROR"]),
    ]
)

result = keyed_transpose(
    source, [agg.count_("Count")], ["NodeId"], ["Level"], init_groups
)
```

### Handle new column-by values

By default, a ticking result fails with an error if the source receives a column-by value that was in neither the source nor `initial_groups` when the operation ran. To ignore such values instead, set `new_column_behavior` to [`NewColumnBehaviorType.IGNORE`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.NewColumnBehaviorType). The result then has no column for the new value, and the rows that contain it don't contribute to any cell.

In this example, every fourth log entry has the level `DEBUG`, which is not in `init_groups`. With `IGNORE`, the result keeps the `INFO`, `WARN`, and `ERROR` columns and leaves out the `DEBUG` entries:

```python ticking-table order=null
from deephaven import agg, new_table, time_table
from deephaven.column import int_col, string_col
from deephaven.table import keyed_transpose, NewColumnBehaviorType

source = time_table("PT1s").update(
    [
        "NodeId = ii % 2 == 0 ? 10 : 20",
        "Level = ii % 4 == 3 ? `DEBUG` : (ii % 3 == 0 ? `WARN` : `INFO`)",
    ]
)

init_groups = new_table(
    [
        int_col("NodeId", [10, 10, 10, 20, 20, 20]),
        string_col("Level", ["INFO", "WARN", "ERROR", "INFO", "WARN", "ERROR"]),
    ]
)

result = keyed_transpose(
    source,
    [agg.count_("Count")],
    ["NodeId"],
    ["Level"],
    init_groups,
    new_column_behavior=NewColumnBehaviorType.IGNORE,
)
```

## Common patterns

The following examples apply a single aggregation and a single column-by column to common long-format datasets.

### Time-series metrics

```python order=metrics_data,result
from deephaven import agg, new_table
from deephaven.column import string_col, double_col
from deephaven.table import keyed_transpose

metrics_data = new_table(
    [
        string_col("Timestamp", ["10:00", "10:00", "10:01", "10:01", "10:02", "10:02"]),
        string_col("Metric", ["CPU", "Memory", "CPU", "Memory", "CPU", "Memory"]),
        double_col("Value", [45.2, 78.5, 52.1, 80.3, 48.7, 79.1]),
    ]
)

# Transform: Timestamp | Metric | Value
# Into: Timestamp | CPU | Memory
result = keyed_transpose(metrics_data, [agg.last(["Value"])], ["Timestamp"], ["Metric"])
```

### Survey responses

The aggregation doesn't have to be numeric. This example uses [`agg.first`](../reference/table-operations/group-and-aggregate/AggFirst.md) to place each text answer in its question's column:

```python order=survey_data,result
from deephaven import agg, new_table
from deephaven.column import int_col, string_col
from deephaven.table import keyed_transpose

survey_data = new_table(
    [
        int_col("RespondentId", [1, 1, 1, 2, 2, 2, 3, 3, 3]),
        string_col("Question", ["Q1", "Q2", "Q3", "Q1", "Q2", "Q3", "Q1", "Q2", "Q3"]),
        string_col(
            "Answer", ["Yes", "No", "Maybe", "No", "Yes", "Yes", "Yes", "Yes", "No"]
        ),
    ]
)

# Transform: RespondentId | Question | Answer
# Into: RespondentId | Q1 | Q2 | Q3
result = keyed_transpose(
    survey_data, [agg.first(["Answer"])], ["RespondentId"], ["Question"]
)
```

## Best practices

- **Column count**: [`keyed_transpose`](../reference/table-operations/format/keyed-transpose.md) creates one output column per unique combination of column-by values, times the number of aggregation output columns. Avoid high-cardinality column-by columns, which produce very wide tables.
- **Initial groups**: For a ticking source, use `initial_groups` to create every expected column when the operation runs.
- **New column-by values**: Set `new_column_behavior` to `NewColumnBehaviorType.IGNORE` if the source can receive column-by values that aren't in `initial_groups`.
- **Aggregation choice**: Choose aggregations that make sense for your data. Common choices include [`agg.count_`](../reference/table-operations/group-and-aggregate/AggCount.md), [`agg.sum_`](../reference/table-operations/group-and-aggregate/AggSum.md), [`agg.avg`](../reference/table-operations/group-and-aggregate/AggAvg.md), [`agg.first`](../reference/table-operations/group-and-aggregate/AggFirst.md), and [`agg.last`](../reference/table-operations/group-and-aggregate/AggLast.md).

## Related documentation

- [Multi-aggregation](./combined-aggregations.md)
- [Table types](../conceptual/table-types.md)
- [`keyed_transpose`](../reference/table-operations/format/keyed-transpose.md)
- [Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.keyed_transpose)
