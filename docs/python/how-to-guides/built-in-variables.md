---
title: Built-in query language variables
sidebar_label: Built-in Variables
---

There are three special built-in query language variables worth noting. They correspond to row indices in tables.

- `i` is a 32-bit integer representing the current row index.
- `ii` is a 64-bit integer representing the current row index.
- `k` is a 64-bit integer representing a special internal indexing value.

`i` and `ii` can be used to access the current, previous, and subsequent rows in a table.

> [!WARNING]
> `k` is a Deephaven engine index and does not correspond to traditional row indices. It should only be used in limited circumstances, such as debugging or advanced query operations.

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

For refreshing tables where you need to reference preceding or following values, see [Alternatives for refreshing tables](#alternatives-for-refreshing-tables) below.

## Usage

The following code block shows how to use `i` and `ii` in a query:

```python order=source
from deephaven import empty_table

source = empty_table(10).update(
    ["RowIndex32Bit = i", "RowIndex64Bit = ii", "EngineIndex64Bit = k"]
)
```

## Alternatives for refreshing tables

When working with refreshing tables where you need to reference preceding or following column values, avoid using column array notation (e.g., `Column_[ii-1]`). Instead, use one of the following approaches:

The examples below use Iceberg tables with auto-refresh mode, which creates add-only tables in Deephaven. For information on setting up Iceberg, see the [Iceberg guide](./data-import-export/iceberg.md).

### 1. Source partitioned tables

Partition a table into multiple smaller tables. This can make operations more manageable and efficient, especially when dealing with add-only tables.

```python docker-config=iceberg test-set=1 order=null
from deephaven.experimental import iceberg
from deephaven import empty_table

# Create and write an Iceberg table for demonstration
source_data = empty_table(20).update(formulas=["GroupKey = i % 3", "Value = i * 10"])

rest_adapter = iceberg.adapter_s3_rest(
    name="minio-iceberg",
    catalog_uri=catalog_uri,
    warehouse_location=warehouse_location,
    region_name=aws_region,
    access_key_id=aws_access_key_id,
    secret_access_key=aws_secret_access_key,
    end_point_override=s3_endpoint,
)

source_adapter = rest_adapter.create_table(
    table_identifier="examples.partition_source",
    table_definition=source_data.definition,
)

writer_options = iceberg.TableParquetWriterOptions(
    table_definition=source_data.definition
)
source_writer = source_adapter.table_writer(writer_options=writer_options)
source_writer.append(iceberg.IcebergWriteInstructions([source_data]))

# Load the Iceberg table with auto-refresh mode (creates an add-only table)
add_only_source = source_adapter.table(
    update_mode=iceberg.IcebergUpdateMode.auto_refresh()
)

# Partition by a grouping column to create multiple tables
partitioned = add_only_source.partition_by(by=["GroupKey"])
```

Each partition can then be processed independently, and operations within each partition can safely use `i` and `ii` since each partition is a separate table.

### 2. By → update → ungroup pattern

[Group](../reference/table-operations/group-and-aggregate/groupBy.md) the data, perform the update operation within each group, then [ungroup](../reference/table-operations/group-and-aggregate/ungroup.md). This allows you to reference values within each group without relying on absolute row positions.

```python docker-config=iceberg test-set=1 order=result
from deephaven.experimental import iceberg
from deephaven import empty_table

# Create and write an Iceberg table for demonstration
group_data = empty_table(10).update(formulas=["Group = i % 3", "Value = i * 10"])

group_adapter = rest_adapter.create_table(
    table_identifier="examples.group_source", table_definition=group_data.definition
)

group_writer_options = iceberg.TableParquetWriterOptions(
    table_definition=group_data.definition
)
group_writer = group_adapter.table_writer(writer_options=group_writer_options)
group_writer.append(iceberg.IcebergWriteInstructions([group_data]))

# Load the Iceberg table with auto-refresh mode (creates an add-only table)
add_only_group_source = group_adapter.table(
    update_mode=iceberg.IcebergUpdateMode.auto_refresh()
)

# Group by the grouping column and compute previous values using array operations
# Prepend null to match array sizes for ungroup
grouped = add_only_group_source.group_by(by=["Group"]).update(
    "PrevValue = concat(new int[]{NULL_INT}, Value.size() > 0 ? Value.subVector(0, Value.size() - 1).toArray() : new int[0])"
)

# Ungroup to work with individual rows
result = grouped.ungroup()
```

> [!NOTE]
> Be aware that the group_by → update → ungroup pattern can cause tick expansion in ticking tables, where a single update to one row may trigger updates to multiple rows in the result.

### 3. As-of joins

If you have a matching column (such as a timestamp or sequence number) that can be reliably used instead of row position, use an [as-of join](../reference/table-operations/join/aj.md).

```python docker-config=iceberg test-set=1 order=aj_result
from deephaven.experimental import iceberg
from deephaven import empty_table

# Create and write an Iceberg table for demonstration
aj_data = empty_table(10).update(formulas=["Timestamp = (long)i", "Value = i * 10"])

aj_adapter = rest_adapter.create_table(
    table_identifier="examples.aj_source", table_definition=aj_data.definition
)

aj_writer_options = iceberg.TableParquetWriterOptions(
    table_definition=aj_data.definition
)
aj_writer = aj_adapter.table_writer(writer_options=aj_writer_options)
aj_writer.append(iceberg.IcebergWriteInstructions([aj_data]))

# Load the Iceberg table with auto-refresh mode (creates an add-only table)
add_only_aj_source = aj_adapter.table(
    update_mode=iceberg.IcebergUpdateMode.auto_refresh()
)

# Create a shifted version for the join
shifted = add_only_aj_source.view("ShiftedTimestamp = Timestamp + 1")

# Join to get the previous value based on timestamp
aj_result = shifted.aj(
    table=add_only_aj_source,
    on=["ShiftedTimestamp >= Timestamp"],
    joins=["PrevValue = Value"],
)
```

### Performance considerations

The performance characteristics of these approaches depend on your data:

- **[As-of joins](../reference/table-operations/join/aj.md)** require a hash table for each group, with the full set of timestamp and row key data stored individually.
- **[`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) → [`update`](../reference/table-operations/select/update.md) → [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md)** requires a hash table and a rowset.
- The relative performance depends on your specific data patterns and cannot be determined from first principles alone.

Choose the approach that best fits your data structure and access patterns.

## Related documentation

- [Built-in constants](./built-in-constants.md)
- [Built-in functions](./built-in-functions.md)
- [Query string overview](./query-string-overview.md)
- [Filters](./filters.md)
- [Formulas](./formulas.md)
- [Work with arrays](./work-with-arrays.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`update`](../reference/table-operations/select/update.md)
