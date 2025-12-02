---
title: How do I delete historical data?
---

_Can I delete historical data from Deephaven tables? I've heard that Deephaven is an append-only database._

The answer depends on the **type of table** you're working with. While some Deephaven table types are designed to retain all data, others support data deletion.

## In-memory user tables

**Yes, you can delete data** from keyed [input tables](../../how-to-guides/input-tables.md). Input tables are user-created, in-memory tables that support both adding and deleting data.

To delete data from a keyed input table, use the `delete` or `deleteAsync` methods via the [InputTableUpdater](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html). You only need to provide the key values for the rows you want to delete:

```groovy order=myInputTable
import io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable
import io.deephaven.engine.util.input.InputTableUpdater

// Create a keyed input table from source data
source = newTable(
    stringCol("Sym", "A0", "A1", "A2"),
    doubleCol("Price", 10.0, 20.0, 30.0)
)

myInputTable = KeyedArrayBackedInputTable.make(source, "Sym")

// Get the updater to add/delete data
updater = (InputTableUpdater)myInputTable.getAttribute(Table.INPUT_TABLE_ATTRIBUTE)

// Delete a specific row by key using the updater
rowsToDelete = newTable(stringCol("Sym", "A0"))
updater.delete(rowsToDelete)
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

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.sources.ring.RingTableTools
import io.deephaven.engine.table.impl.TimeTable.Builder

// Create a blink table
builder = new Builder().period("PT0.5s").blinkTable(true)
t = builder.build()

// Create ring table that holds only the last 10 rows
tRing = RingTableTools.of(t, 10)
```

Ring tables are ideal for scenarios where you need a sliding window of recent data without unbounded memory growth.

## Blink tables: Data automatically cleared each cycle

[Blink tables](../../conceptual/table-types.md#specialization-3-blink) automatically clear all data after each update cycle, retaining only the most recent batch of rows. This makes them excellent for low-memory streaming applications:

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.TimeTable.Builder

// Create a blink table that only keeps data from the current cycle
builder = new Builder().period("PT0.5s").blinkTable(true)
t = builder.build()
```

Blink tables don't require manual deletion - they automatically discard previous data with each update.

## Partitioned table partitions

For [partitioned tables](../../how-to-guides/partitioned-tables.md), you can remove entire constituent tables (partitions), but you cannot delete individual rows within a partition unless the underlying table type supports it (e.g., if the constituent tables are input tables).

To work with specific partitions:

- Use [`constituentFor`](../table-operations/partitioned-tables/constituentFor.md) to retrieve a specific partition by key.
- Create a new partitioned table excluding unwanted partitions using [`filter`](../table-operations/partitioned-tables/filter.md).

## Summary

| Table Type                      | Can Delete Data? | How?                                                                  |
| ------------------------------- | ---------------- | --------------------------------------------------------------------- |
| **Keyed input tables**          | ✅ Yes           | `delete()` or `deleteAsync()` methods                                 |
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

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
