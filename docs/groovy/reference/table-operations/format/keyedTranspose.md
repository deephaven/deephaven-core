---
title: keyedTranspose
---

The `keyedTranspose` operation takes a source table with a set of aggregations and produces a new table where the values are aggregated according to the `rowByColumns` and `columnByColumns`. Each unique combination of the columns specified in `rowByColumns` produce a new row, and each unique combination of values for the columns specified in `columnByColumns` are used for the column names. An optional set of `initialGroups` can be provided to ensure that the output table contains the full set of aggregated columns, even if no data is present yet in the source table.

## Syntax

```
keyedTranspose(source, aggregations, rowByColumns, columnByColumns)
keyedTranspose(source, aggregations, rowByColumns, columnByColumns, initialGroups)
keyedTranspose(source, aggregations, rowByColumns, columnByColumns, initialGroups, newColumnBehavior)
```

## Parameters

<ParamTable>
<Param name="source" type="Table">

The source table to transpose.

</Param>
<Param name="aggregations" type="Collection<? extends Aggregation>">

The aggregations to apply to the source table.

</Param>
<Param name="rowByColumns" type="Collection<? extends ColumnName>">

The columns to use as row keys in the transposed table.

</Param>
<Param name="columnByColumns" type="Collection<? extends ColumnName>">

The columns whose values become the new aggregated columns.

</Param>
<Param name="initialGroups" type="Table">

An optional initial set of groups to ensure all aggregated columns are present in the output.

</Param>
<Param name="newColumnBehavior" type="NewColumnBehavior">

The behavior when a new column would be added, because it was encountered later in the data.

</Param>
</ParamTable>

## Returns

The transposed table.

## Example

In this example start with a two-column table that has a `Date` and a `Level`. The `Level` column's values are used as column names in the result and aggregated according to the `Date` key.

```groovy order=result,source
import io.deephaven.engine.table.impl.util.KeyedTranspose

source = newTable(
    stringCol("Date", "2025-08-05", "2025-08-05", "2025-08-06", "2025-08-07"),
    stringCol("Level", "INFO", "INFO", "WARN", "ERROR")
)
result = KeyedTranspose.keyedTranspose(
    source, List.of(AggCount("Count")),
    ColumnName.from("Date"), ColumnName.from("Level")
)
```

## Example with Initial Groups

The `initialGroups` parameter ensures that `columnByColumns` values appear as columns whether or not they have any data to aggregate. Some ticking data sets may have no data for these columns at the time the transposed table is initialized but will have data for those columns later. The following example shows two transpositions of the same table; one that specifies `initialGroups` and one that does not. The `initialGroups` parameter is specified as a Deephaven `Table` and represents all of the combinations of `columnByColumns` values that are expected in the data.

```groovy order=result1,result2,source
import io.deephaven.engine.table.impl.util.KeyedTranspose

source = newTable(
    stringCol("Date", "2025-08-05", "2025-08-05", "2025-08-06", "2025-08-07"),
    stringCol("Level", "INFO", "INFO", "WARN", "ERROR"),
    intCol("NodeId", 10, 10, 10, 10)
)

result1 = KeyedTranspose.keyedTranspose(
    source, List.of(AggCount("Count")), ColumnName.from("Date"),
    ColumnName.from("Level", "NodeId")
)

initGroups = newTable(
    stringCol("Level", "ERROR", "WARN", "INFO", "INFO", "WARN", "ERROR"),
    intCol("NodeId", 10, 10, 10, 20, 20, 20),
).join(source.selectDistinct("Date"))

result2 = KeyedTranspose.keyedTranspose(
    source, List.of(AggCount("Count")), ColumnName.from("Date"),
    ColumnName.from("Level", "NodeId"), initGroups
)
```

## Column Naming Conventions

In the above example, you can see that the column names (e.g. INFO, WARN, ERROR) are taken from the values occurring for `Level`. But what if there are multiple `aggregations` or multiple `columnByColumns` specified? The resulting column names may yield duplicates.

To avoid conflicts, the column naming works according to the following contract:

- If `aggregations = 1` and `columnByColumns = 1`: Column names are the value of the `columnByColumns` column. (ex. INFO, WARN)
- If `aggregations > 1`: Column names are prefixed with the aggregation column name. (ex. Count_INFO, MySum_INFO)
- If `columnByColumns > 1`: Values for the original columns are separated by an underscore (ex. INFO_OTHER1, WARN_OTHER2)
- If Illegal Characters: Purge characters that are invalid for Deephaven column names. (ex. "1-2.3/4" becomes "1234")
- If Starts with Number: Add the prefix "column\_" to the column name. (ex. column_123)
- If Duplicate Column Name: Add a suffix to differentiate the columns. (ex. INFO, INFO2)

Given the above contract, and to give you more control over the result, it may be necessary to sanitize data values that may be used as column names before using `keyedTranspose`. Otherwise, "12.34" could be translated to "column_1234" instead of a more meaningful column name.

## Related documentation

- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedTranspose.html)
