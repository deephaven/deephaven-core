---
title: Predicate Pushdown Filtering
---

Predicate pushdown is a powerful feature in Deephaven that allows you to filter data closer to the data sources, improving query performance by reducing the amount of data that needs to be processed. This guide explains how to use predicate pushdown filtering effectively.

In general, the filtering engine processes user-supplied filters sequentially in the order specified. The first filter operates on the rows of the full table and produces a subset of rows that pass the first filter. This subset is then provided to the subsequent filters and refined until only rows that pass every filter remain.

Predicate pushdown enhances row elimination by leveraging the underlying data sources of the table. For example, match and range filters can use storage file metadata (such as min/max statistics) to eliminate entire row groups or files, so the engine does not need to load unnecessary data from storage. This is especially useful for large datasets stored in formats like Parquet and Iceberg. While filters are processed sequentially, the pushdown mechanism may reorder filters for efficiency based on the underlying data sources and their capabilities—the order of pushed-down filters is not guaranteed.

Filters are prioritized for pushdown in the following order (from highest to lowest):

- Filtering [single value column sources](#single-value-column-sources).
- Range and match filtering of Parquet data columns using [row group metadata](#parquet-row-group-metadata).
- Filtering columns with an existing Deephaven [data index](#deephaven-data-indexes).

> [!IMPORTANT]
> Where multiple filters have the same pushdown priority, the user-supplied order will generally be maintained. Stateful filters and filter barriers are always respected during pushdown operations.

## Single value column sources

Internally, Deephaven often creates single-value column sources to represent constant values in a table. In the following example, the `Value` column of `source` is created as a single-value column source containing `42` for all rows in the table. When filtering on `Value`, such as `Value = 42`, the engine performs a single comparison against the value and then includes or excludes all rows in the source table based on the result. This is a very efficient operation as it does not require scanning the entire table.

```groovy order=source,result
source = emptyTable(1000000000).updateView("X = i", "Value = 42")
result = source.where("Value = 42")
```

## Parquet row group metadata

When processing range or match filters and the source data is loaded from Parquet files, Deephaven can use the Parquet metadata to eliminate entire row groups or files. This is done by checking the minimum and maximum values of the columns in each row group against the filter criteria. If a row group does not contain any rows that match the filter, it is skipped entirely.

This is especially efficient for ordered data, such as sequential timestamps.

```groovy order=source,result
import io.deephaven.parquet.table.ParquetTools

source = ParquetTools.readTable("/data/examples/ParquetExamples/grades/grades.parquet")
result = source.where("Test1 > 90")
```

Parquet metadata is optional, and not all Parquet files will have it. If the metadata is not available, Deephaven will fall back to scanning the data in the row groups to apply the filter. If Parquet metadata is malformed or incorrect, it may lead to incorrect filtering results. In such cases, you can [disable](#disabling-predicate-pushdown-features) this optimization.

## Deephaven data indexes

Deephaven allows users to create data indexes when writing data to storage as Parquet files. These indexes can be used to speed up filtering operations by applying the filter to the index instead of the larger table. This technique is effective even if only a subset of the data files are indexed. The engine will filter non-indexed files using the standard method.

```groovy order=source,disk_table,filtered_1,filtered_2
import io.deephaven.parquet.table.ParquetTools
import io.deephaven.parquet.table.ParquetInstructions

// create a table with random data
source = emptyTable(1000).update("X = randomInt(0, 10)", "Y = randomInt(0, 20)")

// write the table to a Parquet file with index on the 'X' column and a multi-column index on 'X' & 'Y'
instructions = ParquetInstructions.builder()
        .addIndexColumns("X")
        .addIndexColumns("X", "Y")
        .build()
ParquetTools.writeTable(source, "/data/indexed.parquet", instructions)

// load the table and the indexes from disk
disk_table = ParquetTools.readTable("/data/indexed.parquet")

// these filters will use the data index to quickly filter the rows at the file level
filtered_1 = disk_table.where("X = 5")
filtered_2 = disk_table.where("X <= 3 && Y > 10")
```

If desired, you can [disable](#disabling-predicate-pushdown-features) the use of these file-level data indexes during pushdown operations:

## Disabling predicate pushdown features

Under certain circumstances, you may want to disable specific predicate pushdown features. These settings are global and will affect all pushdown operations across the Deephaven engine. The following properties affect pushdown:

- [`QueryTable.disableWherePushdownParquetRowGroupMetadata`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html#DISABLE_WHERE_PUSHDOWN_PARQUET_ROW_GROUP_METADATA) – disables consideration of Parquet row group metadata when filtering.
- [`QueryTable.disableWherePushdownDataIndex`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html#DISABLE_WHERE_PUSHDOWN_DATA_INDEX) – disables the use of Deephaven data indexes when filtering.

For more information, see the [Query table configuration](../conceptual/query-table-configuration.md) documentation.

## Related documentation

- [Filter table data](./use-filters.md)
- [Export to Parquet](./data-import-export/parquet-export.md)
- [Import to Parquet](./data-import-export/parquet-import.md)
- [`getDataIndex`](../reference/engine/getDataIndex.md)
- [`getOrCreateDataIndex`](../reference/engine/getOrCreateDataIndex.md)
- [`hasDataIndex`](../reference/engine/hasDataIndex.md)
- [`DataIndex` Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/DataIndex.html)
