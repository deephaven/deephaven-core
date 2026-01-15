---
title: Built-in query language variables
sidebar_label: Built-in variables
---

There are three special built-in query language variables worth noting. They correspond to row indices in tables.

- `i` is a 32-bit integer representing the current row index.
- `ii` is a 64-bit integer representing the current row index.
- `k` is a 64-bit integer representing a special internal indexing value.

`i` and `ii` can be used to access the current, previous, and subsequent rows in a table.

> [!NOTE] > `k` is a Deephaven engine index and does not correspond to traditional row indices. It should only be used in limited circumstances, such as debugging or advanced query operations.

### Refreshing table restrictions

The engine validates usage of these variables and throws an `IllegalArgumentException` if used unsafely on refreshing tables:

```
IllegalArgumentException: Formula '<formula>' uses i, ii, k, or column array variables,
and is not safe to refresh. Note that some usages, such as on an append-only table are safe.
```

The following table summarizes when each variable is safe to use:

| Variable                        | Safe on                    | Throws error on                                |
| ------------------------------- | -------------------------- | ---------------------------------------------- |
| `i`, `ii`                       | static, append-only, blink | other refreshing tables (add-only, ticking)    |
| `k`                             | static, add-only, blink    | other refreshing tables (append-only, ticking) |
| Column arrays (`Column_[ii-1]`) | static, blink              | any refreshing table (including append-only)   |

For add-only tables where you need to reference preceding or following values, see [Alternatives for add-only tables](#alternatives-for-add-only-tables) below.

## Usage

The following code block shows how to use `i` and `ii` in a query:

```groovy order=source
source = emptyTable(10).update(
    "RowIndex32Bit = i", "RowIndex64Bit = ii", "EngineIndex64Bit = k"
)
```

## Alternatives for add-only tables

When working with add-only tables where you need to reference preceding or following column values, avoid using `i` and `ii` with array notation. Instead, use one of the following approaches:

The examples below use Iceberg tables with auto-refresh mode, which creates add-only tables in Deephaven. For information on setting up Iceberg, see the [Iceberg guide](./data-import-export/iceberg.md).

### 1. Source partitioned tables

Partition a table into multiple smaller tables. This can make operations more manageable and efficient, especially when dealing with add-only tables.

```groovy docker-config=iceberg test-set=1 order=null
import io.deephaven.iceberg.util.*

// Create and write an Iceberg table for demonstration
sourceData = emptyTable(20).update("GroupKey = i % 3", "Value = i * 10")

restAdapter = IcebergToolsS3.createS3Rest(
    "minio-iceberg",
    catalogUri,
    warehouseLocation,
    awsRegion,
    awsAccessKeyId,
    awsSecretAccessKey,
    s3Endpoint
)

sourceAdapter = restAdapter.createTable("examples.partitionSource", sourceData.getDefinition())

writerOptions = TableParquetWriterOptions.builder()
    .tableDefinition(sourceData.getDefinition())
    .build()

sourceWriter = sourceAdapter.tableWriter(writerOptions)
sourceWriter.append(IcebergWriteInstructions.builder().addTables(sourceData).build())

// Load the Iceberg table with auto-refresh mode (creates an add-only table)
autoRefreshInstructions = IcebergReadInstructions.builder()
    .updateMode(IcebergUpdateMode.autoRefreshingMode())
    .build()
addOnlySource = sourceAdapter.table(autoRefreshInstructions)

// Partition by a grouping column to create multiple tables
partitioned = addOnlySource.partitionBy("GroupKey")
```

Each partition can then be processed independently, and operations within each partition can safely use `i` and `ii` since each partition is a separate table.

### 2. By → update → ungroup pattern

[Group](../reference/table-operations/group-and-aggregate/groupBy.md) the data, perform the update operation within each group, then [ungroup](../reference/table-operations/group-and-aggregate/ungroup.md). This allows you to reference values within each group without relying on absolute row positions.

```groovy docker-config=iceberg test-set=1 order=result
import io.deephaven.iceberg.util.*

// Create and write an Iceberg table for demonstration
groupData = emptyTable(10).update("Group = i % 3", "Value = i * 10")

groupAdapter = restAdapter.createTable("examples.groupSource", groupData.getDefinition())

groupWriterOptions = TableParquetWriterOptions.builder()
    .tableDefinition(groupData.getDefinition())
    .build()

groupWriter = groupAdapter.tableWriter(groupWriterOptions)
groupWriter.append(IcebergWriteInstructions.builder().addTables(groupData).build())

// Load the Iceberg table with auto-refresh mode (creates an add-only table)
addOnlyGroupSource = groupAdapter.table(autoRefreshInstructions)

// Group by the grouping column and compute previous values using array operations
// Prepend null to match array sizes for ungroup
grouped = addOnlyGroupSource.groupBy("Group").update(
    "PrevValue = concat(new int[]{NULL_INT}, Value.size() > 0 ? Value.subVector(0, Value.size() - 1).toArray() : new int[0])"
)

// Ungroup to work with individual rows
result = grouped.ungroup()
```

> [!NOTE]
> Be aware that the groupBy → update → ungroup pattern can cause tick expansion in ticking tables, where a single update to one row may trigger updates to multiple rows in the result.

### 3. As-of joins

If you have a matching column (such as a timestamp or sequence number) that can be reliably used instead of row position, use an [as-of join](../reference/table-operations/join/aj.md).

```groovy docker-config=iceberg test-set=1 order=ajResult
import io.deephaven.iceberg.util.*

// Create and write an Iceberg table for demonstration
ajData = emptyTable(10).update("Timestamp = (long)i", "Value = i * 10")

ajAdapter = restAdapter.createTable("examples.ajSource", ajData.getDefinition())

ajWriterOptions = TableParquetWriterOptions.builder()
    .tableDefinition(ajData.getDefinition())
    .build()

ajWriter = ajAdapter.tableWriter(ajWriterOptions)
ajWriter.append(IcebergWriteInstructions.builder().addTables(ajData).build())

// Load the Iceberg table with auto-refresh mode (creates an add-only table)
addOnlyAjSource = ajAdapter.table(autoRefreshInstructions)

// Create a shifted version for the join
shifted = addOnlyAjSource.view("ShiftedTimestamp = Timestamp + 1")

// Join to get the previous value based on timestamp
ajResult = shifted.aj(addOnlyAjSource, "ShiftedTimestamp >= Timestamp", "PrevValue = Value")
```

### Performance considerations

The performance characteristics of these approaches depend on your data:

- **[As-of joins](../reference/table-operations/join/aj.md)** require a hash table for each group, with the full set of timestamp and row key data stored individually.
- **[`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) → [`update`](../reference/table-operations/select/update.md) → [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md)** requires a hash table and a rowset.
- The relative performance depends on your specific data patterns and cannot be determined from first principles alone.

Choose the approach that best fits your data structure and access patterns.

## Related documentation

- [Built-in constants](./built-in-constants.md)
- [Built-in functions](./built-in-functions.md)
- [Query string overview](./query-string-overview.md)
- [Filters](./filters.md)
- [Formulas](./formulas.md)
- [Work with arrays](./work-with-arrays.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`update`](../reference/table-operations/select/update.md)
