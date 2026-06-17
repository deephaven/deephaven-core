---
title: Predicate Pushdown Filtering
---

Predicate pushdown is a powerful feature in Deephaven that allows you to filter data closer to the data sources, improving query performance by reducing the amount of data that needs to be processed. This guide explains how to use predicate pushdown filtering effectively.

## Overview

The filtering engine processes user-supplied filters sequentially in the order you specify. The first filter evaluates the rows of the full table and produces a subset of passing rows. The engine then passes this subset to subsequent filters, refining the data until only rows that satisfy every filter remain.

Storage systems often divide tables into multiple regions. For instance, multiple files frequently compose a Parquet dataset, and multiple row groups further divide each individual file. Storage engines can apply specific optimization techniques to individual files and even to distinct data regions within those files.

Predicate pushdown enhances row elimination by leveraging these underlying data structures. For example, match and range filters may use storage file metadata (such as min/max statistics) to eliminate entire row groups or files. Consequently, the engine avoids loading unnecessary data from storage — a massive advantage for large datasets in formats like Parquet and Iceberg. Although the engine generally processes filters sequentially, the pushdown mechanism may reorder them to maximize efficiency based on the capabilities of the data sources. Therefore, you cannot guarantee the final execution order of pushed-down filters. To enforce a specific order, use [barriers and serial execution](https://deephaven.io/core/docs/conceptual/query-engine/parallelization/#controlling-concurrency-for-select-update-and-where).

## Filter Priority

Filters are prioritized for pushdown in the following order (from highest to lowest):

- Filtering [single value columns](#single-value-columns-and-regions).
- Range and match filtering of a sorted column using [sorted data binary search](#sorted-data-binary-search).
- Filtering columns with a materialized Deephaven [data index](#deephaven-data-indexes).
- Filtering [single value regions](#single-value-columns-and-regions).
- Filtering regions with a materialized Deephaven [data index](#deephaven-data-indexes).
- Range and match filtering of Parquet data regions using [row group metadata](#parquet-row-group-metadata).
- Filtering regions with an un-cached Deephaven [data index](#deephaven-data-indexes).
- Range and match filtering of a sorted region using [sorted data binary search](#sorted-data-binary-search).
- Filtering regions with [dictionary encoding](#dictionary-encoding).

> [!IMPORTANT]
> Where multiple filters have the same pushdown priority, the user-supplied order will generally be maintained. Stateful filters and filter barriers are always respected during pushdown operations.

## Single value columns and regions

Internally, Deephaven often creates single-value column sources to represent constant values in a table. In the following example, the `Value` column of `source` is created as a single-value column source containing `42` for all rows in the table. When filtering on `Value`, such as `Value = 42`, the engine performs a single comparison against the value and then includes or excludes all rows in the source table based on the result. This is a very efficient operation as it does not require scanning the entire table.

```python order=source,result
from deephaven import empty_table

source = empty_table(1_000).update_view(formulas=["X = i", "Value = 42"])
result = source.where(filters="Value = 42")
```

When working with partitioned data, such as Parquet files, regions of the overall table will have a constant value. The engine will also filter these regions efficiently with a single comparison of the constant value,

## Sorted data binary search

Columns or regions with data stored in sorted order (ascending or descending) can efficiently evaluate range and match filters. Rather than scanning every row to evaluate the filter, the engine performs binary search, drastically reducing the number of values to be examined.

Deephaven detects sorted order for a column or region using table metadata. When the Deephaven engine sorts a table or loads a table from storage, the metadata will automatically be set.

```python order=source,sorted_source,result_range,result_match
from deephaven import empty_table

# create a table with random integer and double data
source = empty_table(100).update(
    formulas=["X = randomInt(0, 100)", "Y = randomDouble(0, 100)"]
)

# sort the table by the integer column
sorted_source = source.sort(order_by="X")

# match and range filters will use binary search on the sorted column
result_range = sorted_source.where(filters=["X > 50", "X < 75"])
result_match = sorted_source.where(filters=["X in 50,51,52,53,54,55,56,57,58,59"])
```

### Asserting sorted column metadata

When sorted-column metadata is missing, you can explicitly annotate the table using [`Table.assert_sorted`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.Table.assert_sorted) or [`Table.with_order_for_column`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.Table.with_order_for_column). Both return a new table with the sorted-column attribute set, telling the engine that binary search is safe to use for that column.

> [!NOTE]
> These methods only annotate the table — they do not sort the data. If the column is not actually sorted in the declared order, filters may return incorrect results.

**Prefer `assert_sorted`**: unlike `with_order_for_column`, it validates the sort order at call time for static tables and installs a listener that re-validates every subsequent update for refreshing tables. This catches data errors before they cause silent query bugs.

```python order=source,asserted,result_range,result_match
from deephaven import empty_table
from deephaven.table import SortDirection

# create a table with sorted integer, random double columns
source = empty_table(100).update(formulas=["X = i", "Y = randomDouble(0, 100)"])

# assert that X is sorted ascending — validates the order and enables binary search
asserted = source.assert_sorted("X", order=SortDirection.ASCENDING)

# match and range filters will use binary search on the sorted column
result_range = asserted.where(filters=["X > 50", "X < 75"])
result_match = asserted.where(filters=["X in 50,51,52,53,54,55,56,57,58,59"])
```

If you are certain the data is sorted and want to skip validation entirely, use `with_order_for_column` instead:

```python order=source,annotated,result_range,result_match
from deephaven import empty_table
from deephaven.table import SortDirection

source = empty_table(100).update(formulas=["X = i", "Y = randomDouble(0, 100)"])

annotated = source.with_order_for_column("X", order=SortDirection.ASCENDING)

result_range = annotated.where(filters=["X > 50", "X < 75"])
result_match = annotated.where(filters=["X in 50,51,52,53,54,55,56,57,58,59"])
```

If desired, you can [disable](#disabling-predicate-pushdown-features) the use of sorted column binary search during pushdown operations.

## Deephaven data indexes

Deephaven allows users to create data indexes for any table. These indexes can be retained in memory or written to storage and can speed up filtering operations significantly by applying the filter to the index instead of the larger table.

Starting in Deephaven v0.40.0, the predicate pushdown framework enables data indexes to be used with most filter types (not just exact matches). When a materialized (in-memory) data index for a table exists, the engine can leverage it during `where` operations. To avoid unexpected memory usage, filter operations do not automatically materialize table-level data indexes. However, if an individual file-level data index is available on disk, the engine will use it to filter data without loading the entire index into memory.

This technique is effective even if only a subset of the data files are indexed. The engine will filter non-indexed files using the standard method.

```python order=source,disk_table,filtered_1,filtered_2
from deephaven import empty_table
from deephaven import parquet

# create a table with random data
source = empty_table(1000).update(
    formulas=["X = randomInt(0, 10)", "Y = randomInt(0, 20)"]
)

# write the table to a Parquet file with index on the 'X' column and a multi-column index on 'X' & 'Y'
parquet.write(source, path="/data/indexed.parquet", index_columns=[["X"], ["X", "Y"]])

# load the table and the indexes from disk
disk_table = parquet.read(path="/data/indexed.parquet")

# these filters will use the data index to quickly filter the rows at the file level
filtered_1 = disk_table.where(filters="X = 5")
filtered_2 = disk_table.where(filters="X <= 3 && Y > 10")
```

If desired, you can [disable](#disabling-predicate-pushdown-features) the use of these file-level data indexes during pushdown operations:

## Parquet row group metadata

When processing range or match filters and the source data is loaded from Parquet files, Deephaven can use the Parquet metadata to eliminate entire row groups or files. This is done by checking the minimum and maximum values of the columns in each row group against the filter criteria. If a row group does not contain any rows that match the filter, it is skipped entirely.

This is especially efficient for ordered data, such as sequential timestamps.

```python order=source,result
from deephaven import parquet

# pass the path of the local Parquet file to `read`
source = parquet.read(path="/data/examples/ParquetExamples/grades/grades.parquet")
result = source.where(filters="Test1 > 90")
```

Parquet metadata is optional, and not all Parquet files will have it. If the metadata is not available, Deephaven will fall back to scanning the data in the row groups to apply the filter. If Parquet metadata is malformed or incorrect, it may lead to incorrect filtering results. In such cases, you can [disable](#disabling-predicate-pushdown-features) this optimization.

## Dictionary encoding

When storing `string` data, Deephaven and other engines may create a dictionary encoding for the column where a dictionary of unique values is stored separately from the column data. The column data contains references (e.g., integer indices) to the dictionary entries instead of the actual string values. This can significantly reduce storage space and improve performance for columns with many repeated values. This additionally allows for efficient filtering on these columns, as the engine can check the unique dictionary values against the filter to determine matches without scanning the entire column. If matches are found, the engine will note which integer indices in the column data correspond to the matching dictionary values and filter the data much more efficiently than loading each string value and applying the filter. Additionally, if no matches are found, the engine can skip the entire column without scanning any of the row data.

Nearly all single-column filters can be optimized using the dictionary encoding.

```python order=source,result
from deephaven import parquet

# pass the path of the local Parquet file to `read`
source = parquet.read(path="/data/examples/ParquetExamples/grades/grades.parquet")
result = source.where(filters="Class = `Math`")
```

If desired, you can [disable](#disabling-predicate-pushdown-features) the use of dictionary encoding during pushdown operations:

## Disabling predicate pushdown features

Under certain circumstances, you may want to disable specific predicate pushdown features. These settings are global and will affect all pushdown operations across the Deephaven engine. The following properties affect pushdown:

- [`QueryTable.useDataIndexForWhere`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html#USE_DATA_INDEX_FOR_WHERE) – enables the use of Deephaven table-level data indexes when filtering.
- [`QueryTable.disableWherePushdownParquetRowGroupMetadata`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html#DISABLE_WHERE_PUSHDOWN_PARQUET_ROW_GROUP_METADATA) – disables consideration of Parquet row group metadata when filtering.
- [`QueryTable.disableWherePushdownDataIndex`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html#DISABLE_WHERE_PUSHDOWN_DATA_INDEX) – disables the use of file-level Deephaven data indexes when filtering.
- [`QueryTable.disableWherePushdownDictionary`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html#DISABLE_WHERE_PUSHDOWN_DICTIONARY) – disables the use of dictionary encoding when filtering.
- [`QueryTable.disableWherePushdownSortedColumn`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html#DISABLE_WHERE_PUSHDOWN_SORTED_COLUMN_LOCATION) – disables the use of sorted column binary search when filtering.

For more information, see the [Query table configuration](../conceptual/query-table-configuration.md) documentation.

## Related documentation

- [Filter table data](./use-filters.md)
- [Export to Parquet](./data-import-export/parquet-export.md)
- [Import to Parquet](./data-import-export/parquet-import.md)
- [`data_index`](../reference/engine/data-index.md)
- [`has_data_index`](../reference/engine/has-data-index.md)
- [`DataIndex` Pydoc](/core/pydoc/code/deephaven.experimental.data_index.html#deephaven.experimental.data_index.DataIndex)
