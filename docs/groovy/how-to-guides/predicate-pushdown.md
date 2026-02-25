---
title: Predicate Pushdown Filtering
---

Predicate pushdown is a powerful feature in Deephaven that allows you to filter data closer to the data sources, improving query performance by reducing the amount of data that needs to be processed. This guide explains how to use predicate pushdown filtering effectively.

## Overview

In general, the filtering engine processes user-supplied filters sequentially in the order specified. The first filter operates on the rows of the full table and produces a subset of rows that pass the first filter. This subset is then provided to the subsequent filters and refined until only rows that pass every filter remain.

Predicate pushdown enhances row elimination by leveraging the underlying data sources of the table. For example, match and range filters can use storage file metadata (such as min/max statistics) to eliminate entire row groups or files, so the engine does not need to load unnecessary data from storage. This is especially useful for large datasets stored in formats like Parquet and Iceberg. While filters are processed sequentially, the pushdown mechanism may reorder filters for efficiency based on the underlying data sources and their capabilities—the order of pushed-down filters is not guaranteed.

Filters are prioritized for pushdown in the following order (from highest to lowest):

- Filtering [single value column sources](#single-value-column-sources).
- Range and match filtering of Parquet data columns using [row group metadata](#parquet-row-group-metadata).
- Filtering columns with a cached (already loaded in memory) Deephaven [data index](#deephaven-data-indexes).
- Filtering columns with dictionary encoding
- Filtering columns with an un-cached Deephaven [data index](#deephaven-data-indexes).

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


## Parquet dictionary encoding

When storing `string` data, Parquet may create a dictionary encoding for the column where a dictionary of unique values is stored separately from the column data. The column data contains references (e.g., integer indices) to the dictionary entries instead of the actual string values. This can significantly reduce storage space and improve performance for columns with many repeated values. This additionally allows for efficient filtering on these columns, as the engine can check the unique dictionary values against the filter to determine matches without scanning the entire column. If matches are found, the engine will note which integer indices in the column data correspond to the matching dictionary values and filter the dataa much more efficiently than loading each string value and applying the filter. Additionally, if no matches are found, the engine can skip the entire column without scanning any of the row data.

Nearly all single-column filters can be optimized using the dictionary encoding.

```groovy order=source,result
import io.deephaven.parquet.table.ParquetTools

source = ParquetTools.readTable("/data/examples/ParquetExamples/grades/grades.parquet")
result = source.where("Class = `Math`")
```

If desired, you can [disable](#disabling-predicate-pushdown-features) the use of dictionary encoding during pushdown operations:

## Deephaven data indexes

Deephaven allows users to create data indexes for any table. These indexes can be retained in memory or written to storage and can speed up filtering operations significantly by applying the filter to the index instead of the larger table.

Starting in Deephaven v0.40.0, the predicate pushdown framework enables data indexes to be used with most filter types (not just exact matches). When a materialized (in-memory) data index for a table exists, the engine can leverage it during `where` operations. To avoid unexpected memory usage, filter operations do not automatically materialize table-level data indexes. However, if an individual file-level data index is available on disk, the engine will use it to filter data without loading the entire index into memory.

This technique is effective even if only a subset of the data files are indexed. The engine will filter non-indexed files using the standard method.

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
