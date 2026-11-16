---
title: Keyed transpose
---

This guide shows you how to use [`keyed_transpose`](../reference/table-operations/format/keyed-transpose.md) to reshape a table from long format to wide format. In long format, each row holds one observation, such as one log entry with a `Date` and a `Level`. In wide format, each distinct value of a category column, such as each `Level`, becomes its own column. This works like a pivot table, flattened into a single Deephaven table that you can use in further table operations like any other.

[`keyed_transpose`](../reference/table-operations/format/keyed-transpose.md) works on real-time data as well as static data. When the source is a [ticking table](../conceptual/table-types.md), the result keeps updating as new rows arrive, but its set of columns is fixed when the operation runs. [Ticking tables and initial groups](#ticking-tables-and-initial-groups) shows how to plan for that.

## When to use a keyed transpose

Use [`keyed_transpose`](../reference/table-operations/format/keyed-transpose.md) when you need to:

- **Pivot data from long to wide format**: Convert rows of categorical data into columns.
- **Create cross-tabulations**: Build summary tables with aggregated values.
- **Compare series side by side**: Give each metric, sensor, or symbol its own column, with one row per timestamp.
- **Prepare data for visualization**: Produce the one-column-per-series wide format that some charts expect.

## Basic usage

The simplest use case involves specifying:

1. A source table.
2. One or more [aggregations](./combined-aggregations.md) to apply.
3. Columns whose values identify each output row (`row_by_cols`). This guide calls these the _row-by columns_.
4. Columns whose values become new column names (`col_by_cols`). This guide calls these the _column-by columns_.

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
- The operation groups the source rows by each combination of `Date` and `Level`. Each combination is a _group_. The operation applies the [`agg.count_`](../reference/table-operations/group-and-aggregate/AggCount.md) [aggregation](./combined-aggregations.md) to each group and puts the count in that group's cell.
- A combination with no rows in the source, such as `2025-08-06` and `INFO`, has no group, so its cell is null.

## Multiple row-by columns

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
    source, [agg.sum_(["TotalCount = Count"])], ["Date", "Server"], ["Level"]
)
```

Each unique combination of `Date` and `Server` creates a separate row in the output.

## Multiple column-by columns

You can also specify multiple column-by columns. The operation creates one output column for each unique combination of their values and joins the values with an underscore:

```python order=result,source
from deephaven import agg, new_table
from deephaven.column import string_col, int_col
from deephaven.table import keyed_transpose

source = new_table(
    [
        string_col("Date", ["2025-08-05", "2025-08-05", "2025-08-06", "2025-08-06"]),
        string_col("Level", ["INFO", "WARN", "INFO", "INFO"]),
        int_col("NodeId", [10, 10, 10, 20]),
    ]
)

result = keyed_transpose(source, [agg.count_("Count")], ["Date"], ["Level", "NodeId"])
```

The result has one column for each combination of `Level` and `NodeId` in the source: `INFO_10`, `WARN_10`, and `INFO_20`. For how these names interact with the other naming rules, see [Column naming](#column-naming).

## Multiple aggregations

You can apply multiple [aggregations](./combined-aggregations.md) at once. When you do, the operation prefixes each result column name with the output column name of the [aggregation](./combined-aggregations.md) that produced it:

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
    [agg.sum_(["TotalSales = Sales"]), agg.avg(["AvgRevenue = Revenue"])],
    ["Product"],
    ["Region"],
)
```

The resulting columns are `TotalSales_North`, `AvgRevenue_North`, `TotalSales_South`, and `AvgRevenue_South`. The operation groups the columns for each `Region` value together. Within each group, the columns follow the order in which you list the [aggregations](./combined-aggregations.md).

The operation adds this prefix only when you pass two or more [aggregations](./combined-aggregations.md). It counts [aggregations](./combined-aggregations.md), not output columns:

- A single [`agg.sum_(["Sales", "Revenue"])`](../reference/table-operations/group-and-aggregate/AggSum.md) gets no prefix. Its names collide, so they get numeric suffixes, such as `North` and `North2` (see [Duplicate names and reserved words](#duplicate-names-and-reserved-words)).
- Two [aggregations](./combined-aggregations.md), `[agg.sum_(["Sales"]), agg.sum_(["Revenue"])]`, get prefixed names, such as `Sales_North` and `Revenue_North`.

## Column naming

The [`keyed_transpose`](../reference/table-operations/format/keyed-transpose.md) operation builds each output column name in two steps. First, it builds a base name from the column-by values and the [aggregations](./combined-aggregations.md):

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

```python order=scenario1,scenario2,scenario3,source
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

# Scenario 1: Single aggregation, single column-by column
# Result columns: RowKey, Normal, column_1234, column_123, INFO, WARN
scenario1 = keyed_transpose(source, [agg.sum_(["Value"])], ["RowKey"], ["Category"])

# Scenario 2: Multiple aggregations
# Result columns: RowKey, Sum_Normal, Count_Normal, Sum_1234, Count_1234, Sum_123, Count_123, Sum_INFO, Count_INFO, Sum_WARN, Count_WARN
scenario2 = keyed_transpose(
    source, [agg.sum_(["Sum = Value"]), agg.count_("Count")], ["RowKey"], ["Category"]
)

# Scenario 3: Multiple column-by columns
# Result columns: RowKey, Normal_1, column_1234_1, column_123_1, INFO_10, WARN_10, INFO_20, WARN_20
scenario3 = keyed_transpose(
    source, [agg.sum_(["Value"])], ["RowKey"], ["Category", "NodeId"]
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

A generated name that matches a row-by column name doesn't get a suffix. Instead, it causes an error. To avoid the error, rename the row-by column with [`rename_columns`](../reference/table-operations/select/rename-columns.md) or [sanitize the values](#sanitize-data-before-transposing) first.

### Sanitize data before transposing

To control the column names yourself, clean the values before you call [`keyed_transpose`](../reference/table-operations/format/keyed-transpose.md). Without cleaning, `Type-A` becomes `TypeA`. This example replaces the hyphen with an underscore, so the columns are `Type_A` and `Type_B`:

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
    cleaned, [agg.sum_(["Total = Value"])], ["Date"], ["CleanCategory"]
)
```

## Ticking tables and initial groups

When the source is a [ticking table](../conceptual/table-types.md), a table that updates as new data arrives, [`keyed_transpose`](../reference/table-operations/format/keyed-transpose.md) sets its output columns once, at the moment the operation runs. The result still adds, removes, and updates rows, but it never adds columns. This has two consequences:

- If the source has no rows when the operation runs and you don't pass an `initial_groups` table with rows, [`keyed_transpose`](../reference/table-operations/format/keyed-transpose.md) has no column-by values to build columns from, and it raises an error.
- A column-by value that first appears after the operation runs has no column to go in. By default, the result fails with an error. See [Handle new column-by values](#handle-new-column-by-values).

To create the columns up front, pass the `initial_groups` parameter a table that lists every combination of row-by and column-by values that should appear in the result. The table must contain all of the row-by and column-by columns. Each distinct combination of row-by values in it creates a row in the result, even before the source has data for it.

In this example, the source starts empty and never contains an `ERROR` row, but the result has an `ERROR` column from the start. Because the table passed as `initial_groups` creates a group for every combination it lists, the `ERROR` cells hold a count of 0 rather than null:

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

The time table adds a row every second, so the result changes as you watch it. To see a result that stays put, let's swap the time table for an [input table](./input-tables.md), a table that changes only when you add rows to it. The input table starts empty, and the code then adds four rows, none of them `ERROR`:

```python order=result,source
from deephaven import agg, input_table, new_table
from deephaven import dtypes as dht
from deephaven.column import int_col, string_col
from deephaven.table import keyed_transpose

source = input_table({"NodeId": dht.int32, "Level": dht.string})

init_groups = new_table(
    [
        int_col("NodeId", [10, 10, 10, 20, 20, 20]),
        string_col("Level", ["INFO", "WARN", "ERROR", "INFO", "WARN", "ERROR"]),
    ]
)

result = keyed_transpose(
    source, [agg.count_("Count")], ["NodeId"], ["Level"], init_groups
)

source.add(
    new_table(
        [
            int_col("NodeId", [10, 10, 20, 20]),
            string_col("Level", ["INFO", "WARN", "INFO", "INFO"]),
        ]
    )
)
```

The result has one row for each `NodeId` and a column for each `Level` in `init_groups`. The `ERROR` column holds 0 for both nodes, and so does the `WARN` cell for node `20`.

### Handle new column-by values

By default, a [ticking](../conceptual/table-types.md) result fails with an error when the source receives a column-by value that has no column. This happens when:

- A value was in neither the source nor `initial_groups` when the operation ran.
- With several column-by columns, a combination of values is new.
- Without `initial_groups`, a value comes back after all of its rows were removed from the source.

To ignore such values instead, set `new_column_behavior` to [`NewColumnBehaviorType.IGNORE`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.NewColumnBehaviorType). The result then has no column for the new value, and the rows that contain it don't contribute to any cell.

In this example, every fourth log entry has the level `DEBUG`, which is not in the `initial_groups` table. With [`IGNORE`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.NewColumnBehaviorType), the result keeps the `INFO`, `WARN`, and `ERROR` columns and leaves out the `DEBUG` entries:

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

To see the effect without waiting for ticks, this version uses an [input table](./input-tables.md) instead. Two of the four rows it adds have the level `DEBUG`:

```python order=result,source
from deephaven import agg, input_table, new_table
from deephaven import dtypes as dht
from deephaven.column import int_col, string_col
from deephaven.table import keyed_transpose, NewColumnBehaviorType

source = input_table({"NodeId": dht.int32, "Level": dht.string})

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

source.add(
    new_table(
        [
            int_col("NodeId", [10, 10, 20, 20]),
            string_col("Level", ["INFO", "DEBUG", "WARN", "DEBUG"]),
        ]
    )
)
```

The `source` table shows all four rows, but the result has only the `INFO`, `WARN`, and `ERROR` columns. Each node has a count of 1 in one column and 0 in the others, so neither `DEBUG` row is counted.

## Common patterns

The following examples apply a single [aggregation](./combined-aggregations.md) and a single column-by column to common long-format datasets.

### Time-series metrics

This example turns one row per metric reading into one row per `Timestamp`, with a column for each `Metric`. Each timestamp has one reading per metric, so [`agg.last`](../reference/table-operations/group-and-aggregate/AggLast.md) carries that reading into its cell:

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

The [aggregation](./combined-aggregations.md) doesn't have to be numeric. This example uses [`agg.first`](../reference/table-operations/group-and-aggregate/AggFirst.md) to place each text answer in its question's column:

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

- **Column count**: [`keyed_transpose`](../reference/table-operations/format/keyed-transpose.md) creates one output column per unique combination of column-by values, times the number of [aggregation](./combined-aggregations.md) output columns. Avoid high-cardinality column-by columns, which produce very wide tables.
- **Initial groups**: For a [ticking](../conceptual/table-types.md) source, use `initial_groups` to create every expected column when the operation runs. See [Ticking tables and initial groups](#ticking-tables-and-initial-groups).
- **New column-by values**: Set `new_column_behavior` to [`NewColumnBehaviorType.IGNORE`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.NewColumnBehaviorType) if the source can receive column-by values that aren't in `initial_groups`. See [Handle new column-by values](#handle-new-column-by-values).
- **[Aggregation](./combined-aggregations.md) choice**: Use [`agg.count_`](../reference/table-operations/group-and-aggregate/AggCount.md) to count rows, [`agg.sum_`](../reference/table-operations/group-and-aggregate/AggSum.md) or [`agg.avg`](../reference/table-operations/group-and-aggregate/AggAvg.md) to combine numeric values, and [`agg.first`](../reference/table-operations/group-and-aggregate/AggFirst.md) or [`agg.last`](../reference/table-operations/group-and-aggregate/AggLast.md) to carry one value, including text, into each cell.

## Related documentation

- [`keyed_transpose`](../reference/table-operations/format/keyed-transpose.md)
- [Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.keyed_transpose)
- [Multi-aggregation](./combined-aggregations.md)
- [`agg.count_`](../reference/table-operations/group-and-aggregate/AggCount.md)
- [`agg.sum_`](../reference/table-operations/group-and-aggregate/AggSum.md)
- [`agg.avg`](../reference/table-operations/group-and-aggregate/AggAvg.md)
- [`agg.first`](../reference/table-operations/group-and-aggregate/AggFirst.md)
- [`agg.last`](../reference/table-operations/group-and-aggregate/AggLast.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`rename_columns`](../reference/table-operations/select/rename-columns.md)
- [Input tables](./input-tables.md)
- [Table types](../conceptual/table-types.md)
