---
title: keyed_transpose
---

The `keyed_transpose` operation takes a source table with a set of aggregations and produces a new table where the values are aggregated according to the `row_by_columns` and `column_by_columns`. Each unique combination of the columns specified in `row_by_columns` produce a new row, and each unique combination of values for the columns specified in `column_by_columns` are used for the column names. An optional set of `initial_groups` can be provided to ensure that the output table contains the full set of aggregated columns, even if no data is present yet in the source table.

## Syntax

```
keyed_transpose(
    table: Table,
    aggs: Union[Aggregation, Sequence[Aggregation]],
    row_by_cols: Union[str, Sequence[str]],
    col_by_cols: Union[str, Sequence[str]],
    initial_groups: Table = None,
    new_column_behavior: NewColumnBehaviorType = NewColumnBehaviorType.FAIL) -> Table:
```

## Parameters

<ParamTable>
<Param name="source" type="Table">

The source table to transpose.

</Param>
<Param name="aggs" type="Union[Aggregation, Sequence[Aggregation]]">

The aggregations to apply to the source table.

</Param>
<Param name="row_by_cols" type="Union[str, list[str]]">

The columns to use as row keys in the transposed table.

</Param>
<Param name="col_by_cols" type="Union[str, list[str]]">

The columns whose values become the new aggregated columns.

</Param>
<Param name="initial_groups" type="Table">

An optional initial set of groups to ensure all aggregated columns are present in the output.

</Param>
<Param name="new_column_behavior" type="NewColumnBehavior">

The behavior when a new column would be added, because it was encountered later in the data.

</Param>
</ParamTable>

## Returns

The transposed table.

## Example with Basic Syntax

In this example start with a two-column table that has a `Date` and a `Level`. The `Level` column's values are used as column names in the result and aggregated according to the `Date` key.

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

## Example with Initial Groups

The `initial_groups` parameter ensures that `col_by_cols` values appear as columns whether or not they have any data to aggregate. Some ticking data sets may have no data for these columns at the time the transposed table is initialized but will have data for those columns later. The following example shows two transpositions of the same table; one that specifies `initial_groups` and one that does not. The `initial_groups` parameter is specified as a Deephaven `Table` and represents all of the combinations of `col_by_cols` values that are expected in the data.

```python order=result1,result2,source
from deephaven import agg, new_table
from deephaven.column import string_col, int_col
from deephaven.table import keyed_transpose

source = new_table(
    [
        string_col("Date", ["2025-08-05", "2025-08-05", "2025-08-06", "2025-08-07"]),
        string_col("Level", ["INFO", "INFO", "WARN", "ERROR"]),
        int_col("NodeId", [10, 10, 10, 10]),
    ]
)

result1 = keyed_transpose(source, [agg.count_("Count")], ["Date"], ["Level", "NodeId"])

init_groups = new_table(
    [
        string_col("Level", ["ERROR", "WARN", "INFO", "INFO", "WARN", "ERROR"]),
        int_col("NodeId", [10, 10, 10, 20, 20, 20]),
    ]
).join(source.select_distinct(["Date"]))

result2 = keyed_transpose(
    source, [agg.count_("Count")], ["Date"], ["Level", "NodeId"], init_groups
)
```

## Column Naming Conventions

In the above example, you can see that the column names (e.g. INFO, WARN, ERROR) are taken from the values occurring for `Level`. But what if there are multiple `aggs` or multiple `column_by_cols` specified? The resulting column names may yield duplicates.

To avoid conflicts, the column naming works according to the following contract:

- If `aggs = 1` and `col_by_cols = 1`: Column names are the value of the `col_by_cols` column (ex. INFO, WARN).
- If `aggs > 1`: Column names are prefixed with the aggregation column name (ex. Count_INFO, MySum_INFO).
- If `col_by_cols > 1`: Values for the original columns are separated by an underscore (ex. INFO_OTHER1, WARN_OTHER2).
- If Illegal Characters: Purge characters that are invalid for Deephaven column names (ex. "1-2.3/4" becomes "1234").
- If Starts with Number: Add the prefix "column\_" to the column name (ex. column_123).
- If Duplicate Column Name: Add a suffix to differentiate the columns (ex. INFO, INFO2).

Given the above contract, and to give you more control over the result, it may be necessary to sanitize data values that may be used as column names before using `keyed_transpose`. Otherwise, "12.34" could be translated to "column_1234" instead of a more meaningful column name.

## Related documentation

- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedTranspose.html)
