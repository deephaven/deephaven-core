---
title: How do I delete historical data?
---

_Can I delete historical data from Deephaven tables? I've heard that Deephaven is an append-only database._

The answer depends on the **type of table** you're working with. While some Deephaven table types are designed to retain all data, others support data deletion.

## In-memory user tables

**Yes, you can delete data** from keyed [input tables](../../how-to-guides/input-tables.md). Input tables are user-created, in-memory tables that support both adding and deleting data.

To delete data from a keyed input table, use the [`delete`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.InputTable.delete) or [`delete_async`](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.InputTable.delete_async) methods on the input table object. You only need to provide the key values for the rows you want to delete:

```python order=my_input_table
from deephaven import empty_table, input_table
from deephaven import dtypes as dht

# Create a keyed input table
column_defs = {"Key": dht.string, "Value": dht.int32}
my_input_table = input_table(col_defs=column_defs, key_cols="Key")

# Add some data
my_input_table.add(empty_table(3).update(["Key = `A` + i", "Value = i * 10"]))

# Delete a specific row by key
my_input_table.delete(empty_table(1).update("Key = `A0`"))

# Or delete asynchronously
my_input_table.delete_async(empty_table(1).update("Key = `A1`"))
```

**Important limitations:**

- Only **keyed** input tables support deletion. Append-only input tables do not.
- You must provide the key column values to identify which rows to delete.

## Standard and append-only tables

**Standard in-memory tables and append-only tables do NOT support row deletion.** These table types are designed to preserve data:

- **Append-only tables**: Rows can only be added to the end. Once added, rows cannot be modified, deleted, or reindexed.
- **Standard streaming tables**: While these tables allow modifications and reindexing, they do not provide a direct deletion API for individual rows.

For these table types, you can:

- Use [`where`](../table-operations/filter/where.md) to filter out unwanted rows (creates a new filtered view, doesn't delete data).
- Use [`head`](../table-operations/filter/head.md) or [`tail`](../table-operations/filter/tail.md) to limit the table size (creates a new view with limited rows).
- Convert to a [ring table](../../how-to-guides/ring-table.md) to automatically discard old data and maintain a fixed capacity.

## Ring tables: Automatic data disposal

[Ring tables](../../how-to-guides/ring-table.md) automatically manage data retention by maintaining a fixed maximum number of rows. As new rows are added, the oldest rows are automatically discarded:

```python ticking-table order=null
from deephaven import time_table, ring_table

# Create a blink table
t = time_table("PT0.5s", blink_table=True)

# Create ring table that holds only the last 10 rows
t_ring = ring_table(t, capacity=10)
```

Ring tables are ideal for scenarios where you need a sliding window of recent data without unbounded memory growth.

## Blink tables: Data automatically cleared each cycle

[Blink tables](../../conceptual/table-types.md#specialization-3-blink) automatically clear all data after each update cycle, retaining only the most recent batch of rows. This makes them excellent for low-memory streaming applications:

```python ticking-table order=null
from deephaven import time_table

# Create a blink table that only keeps data from the current cycle
t = time_table("PT0.5s", blink_table=True)
```

Blink tables don't require manual deletion—they automatically discard previous data with each update.

## Partitioned table partitions

For [partitioned tables](../../how-to-guides/partitioned-tables.md), you can remove entire constituent tables (partitions), but you cannot delete individual rows within a partition unless the underlying table type supports it (e.g., if the constituent tables are input tables).

To work with specific partitions:

- Use [`get_constituent`](../table-operations/partitioned-tables/get-constituent.md) to retrieve a specific partition by key.
- Create a new partitioned table excluding unwanted partitions using [`filter`](../table-operations/partitioned-tables/partitioned-table-filter.md).

## Summary

| Table Type                      | Can Delete Data? | How?                                                                  |
| ------------------------------- | ---------------- | --------------------------------------------------------------------- |
| **Keyed input tables**          | ✅ Yes           | `delete()` or `delete_async()` methods                                |
| **Append-only input tables**    | ❌ No            | Not supported                                                         |
| **Standard/Append-only tables** | ❌ No            | Use filtering or ring tables instead                                  |
| **Ring tables**                 | ✅ Automatic     | Old data discarded automatically when capacity is reached             |
| **Blink tables**                | ✅ Automatic     | Data cleared after each update cycle                                  |
| **Partitioned tables**          | ⚠️ Partial        | Can filter partitions; row deletion depends on constituent table type |

## Related documentation

- [Create and use input tables](../../how-to-guides/input-tables.md)
- [Table types](../../conceptual/table-types.md)
- [Create and use ring tables](../../how-to-guides/ring-table.md)
- [Create and use partitioned tables](../../how-to-guides/partitioned-tables.md)
- [`input_table` API reference](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.input_table)
- [`ring_table` API reference](/core/pydoc/code/deephaven.html#deephaven.ring_table)
- [`time_table` API reference](/core/pydoc/code/deephaven.html#deephaven.time_table)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
