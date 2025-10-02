---
title: Keyed transpose
---

This guide shows you how to use `keyed_transpose` to transform data from a long format to a wide format by pivoting column values into new column names. This is useful when you need to reshape data for analysis, reporting, or visualization.

## When to use keyed_transpose

Use `keyed_transpose` when you need to:

- **Pivot data from long to wide format**: Convert rows of categorical data into columns
- **Create cross-tabulations**: Build summary tables with aggregated values
- **Reshape time-series data**: Transform data where categories are in rows into a format where they become columns
- **Prepare data for visualization**: Many charts require data in wide format

## Basic usage

The simplest use case involves specifying:

1. A source table
2. An aggregation to apply
3. Columns to use as row keys (`row_by_cols`)
4. Columns whose values become new column names (`col_by_cols`)

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
- Each unique `Level` value (INFO, WARN, ERROR) becomes a column.
- The `Count` aggregation counts occurrences for each Date-Level combination.

## Working with multiple row keys

You can specify multiple columns as row keys to create more granular groupings:

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

## Using multiple aggregations

You can apply multiple aggregations simultaneously. When you do this, column names are prefixed with the aggregation name:

```python order=result,source
from deephaven import agg, new_table
from deephaven.column import string_col, int_col, double_col
from deephaven.table import keyed_transpose

source = new_table(
    [
        string_col("Product", ["Widget", "Widget", "Gadget", "Gadget"]),
        string_col("Region", ["North", "South", "North", "South"]),
        int_col("Sales", [100, 150, 200, 175]),
        double_col("Revenue", [1000.0, 1500.0, 2000.0, 1750.0]),
    ]
)

result = keyed_transpose(
    source,
    [agg.sum_(["TotalSales=Sales"]), agg.avg(["AvgRevenue=Revenue"])],
    ["Product"],
    ["Region"],
)
```

The resulting columns will be named like `TotalSales_North`, `TotalSales_South`, `AvgRevenue_North`, `AvgRevenue_South`.

## Handling initial groups for ticking tables

When working with ticking (live updating) tables, you may want to ensure all expected columns exist from the start, even if no data has arrived yet. Use the `initial_groups` parameter:

```python order=result,source
from deephaven import agg, new_table
from deephaven.column import string_col, int_col
from deephaven.table import keyed_transpose

source = new_table(
    [
        string_col("Date", ["2025-08-05", "2025-08-05"]),
        string_col("Level", ["INFO", "INFO"]),
        int_col("NodeId", [10, 10]),
    ]
)

# Define all expected combinations of Level and NodeId
init_groups = new_table(
    [
        string_col("Level", ["ERROR", "WARN", "INFO", "ERROR", "WARN", "INFO"]),
        int_col("NodeId", [10, 10, 10, 20, 20, 20]),
    ]
).join(source.select_distinct(["Date"]))

result = keyed_transpose(
    source, [agg.count_("Count")], ["Date"], ["Level", "NodeId"], init_groups
)
```

Even though the source only has INFO logs from NodeId 10, the result will include columns for all Level-NodeId combinations specified in `init_groups`.

## Understanding column naming

The `keyed_transpose` operation follows specific rules for naming output columns:

| Scenario                             | Column Naming Pattern         | Example                  |
| ------------------------------------ | ----------------------------- | ------------------------ |
| Single aggregation, single column-by | Value from column-by column   | `INFO`, `WARN`           |
| Multiple aggregations                | Aggregation name + value      | `Count_INFO`, `Sum_WARN` |
| Multiple column-by columns           | Values joined with underscore | `INFO_10`, `WARN_20`     |
| Invalid characters                   | Characters removed            | `1-2.3/4` → `1234`       |
| Starts with number                   | Prefixed with `column_`       | `123` → `column_123`     |
| Duplicate names                      | Suffix added                  | `INFO`, `INFO2`          |

### Best practice: Sanitize data before transposing

To maintain control over column names, clean your data values before using `keyed_transpose`:

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

## Common patterns

### Pattern 1: Sales by region and product

```python order=sales_data,result
from deephaven import agg, new_table
from deephaven.column import string_col, int_col
from deephaven.table import keyed_transpose

sales_data = new_table(
    [
        string_col("Product", ["Widget", "Widget", "Gadget", "Gadget", "Widget"]),
        string_col("Region", ["North", "South", "North", "South", "East"]),
        int_col("Sales", [100, 150, 200, 175, 125]),
    ]
)

# Transform: Product | Region | Sales
# Into: Product | North | South | East
result = keyed_transpose(sales_data, [agg.sum_(["Sales"])], ["Product"], ["Region"])
```

### Pattern 2: Time-series metrics

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

### Pattern 3: Survey responses

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

## Tips and considerations

- **Performance**: `keyed_transpose` creates new columns dynamically. For very high-cardinality data (many unique values in `col_by_cols`), this can create tables with many columns.
- **Ticking tables**: Use `initial_groups` to ensure consistent column structure when working with live data.
- **Column limits**: Be mindful of the number of unique values in your `col_by_cols` - each becomes a separate column.
- **Aggregation choice**: Choose aggregations that make sense for your data. Common choices include `agg.count_`, `agg.sum_`, `agg.avg`, `agg.first`, and `agg.last`.

## Related documentation

- [keyed_transpose reference](/python/reference/table-operations/format/keyed-transpose.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedTranspose.html)
- [Aggregations guide](/python/how-to-guides/combined-aggregations.md)
