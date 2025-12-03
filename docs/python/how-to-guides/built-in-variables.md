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
> `k` is a Deephaven engine index and does not correspond to traditional row indices. It is used for Deephaven engine development and should _only_ be used in limited circumstances, such as debugging or advanced query operations.

> [!WARNING]
> These built-in variables are not reliable in ticking tables. They should only be used in static cases.

> [!WARNING]
> Do not use `i` and `ii` in append-only tables to access preceding or following column values using array notation (e.g., `ColA_[ii-1]`). As new rows are added, the row indices shift, causing previously computed values to reference different rows than originally intended. See [Alternatives for append-only tables](#alternatives-for-append-only-tables) below.

## Usage

The following code block shows how to use `i` and `ii` in a query:

```python order=source
from deephaven import empty_table

source = empty_table(10).update(
    ["RowIndex32Bit = i", "RowIndex64Bit = ii", "EngineIndex64Bit = k"]
)
```

## Alternatives for append-only tables

When working with append-only tables where you need to reference preceding or following column values, avoid using `i` and `ii` with array notation. Instead, use one of the following approaches:

### 1. Source partitioned tables

Partition an append-only source table into multiple smaller append-only tables. This can make operations more manageable and efficient.

```python order=source,partitioned
from deephaven import empty_table

source = empty_table(20).update(formulas=["GroupKey = i % 3", "Value = i * 10"])

# Partition by a grouping column to create multiple append-only tables
partitioned = source.partition_by(by=["GroupKey"])
```

Each partition can then be processed independently, and operations within each partition can safely use `i` and `ii` since each partition is a separate table.

### 2. By → update → ungroup pattern

Group the data, perform the update operation within each group, then ungroup. This allows you to reference values within each group without relying on absolute row positions.

```python order=source,grouped,result
from deephaven import empty_table

source = empty_table(10).update(formulas=["Group = i % 3", "Value = i * 10"])

# Group by the grouping column
grouped = source.group_by(by=["Group"])

# Ungroup to work with individual rows, compute previous value within each original group
result = grouped.ungroup().update(formulas=["PrevValue = ii > 0 ? Value_[ii-1] : null"])
```

> [!NOTE]
> Be aware that the by → update → ungroup pattern can cause tick expansion in ticking tables, where a single update to one row may trigger updates to multiple rows in the result.

### 3. As-of joins

If you have a matching column (such as a timestamp or sequence number) that can be reliably used instead of row position, use an as-of join.

```python order=source,shifted,result
from deephaven import empty_table

source = empty_table(10).update(formulas=["Timestamp = (long)i", "Value = i * 10"])

# Create a shifted version for the join
shifted = source.update(formulas=["ShiftedTimestamp = Timestamp + 1"])

# Join to get the previous value based on timestamp
result = shifted.aj(
    table=source, on=["ShiftedTimestamp >= Timestamp"], joins=["PrevValue = Value"]
)
```

### Performance considerations

The performance characteristics of these approaches depend on your data:

- **As-of joins** require a hash table for each group, with the full set of timestamp and row key data stored individually.
- **By → update → ungroup** requires a hash table and a rowset.
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
