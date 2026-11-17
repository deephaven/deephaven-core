---
title: How do I delete historical data?
---

_Can I delete historical data from Deephaven tables? I've heard that Deephaven is an append-only database._

It depends on the **type of table** you're working with. Deephaven tables are views over data held in memory or read from external storage. If you need to remove persisted history (such as Parquet or Iceberg files), you'll need to delete or rewrite it at the source. Within the engine, whether you can delete rows depends on the table type:

## Keyed input tables

**Yes, you can delete data** from keyed [input tables](../../how-to-guides/input-tables.md). Input tables are user-created, in-memory tables that support both adding and deleting data.

To delete data from a keyed input table, use the `delete` or `deleteAsync` methods via the [`InputTableUpdater`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html). Make sure the table you pass to `delete` contains only the key columns — if you include value columns, you'll get an error. To delete rows selected from the input table itself, drop the value columns first with something like `view("Sym")`.

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
updater = InputTableUpdater.from(myInputTable)

// Delete a specific row by key using the updater
rowsToDelete = newTable(stringCol("Sym", "A0"))
updater.delete(rowsToDelete)
```

A couple of things to keep in mind:

- Only **keyed** input tables support deletion. Append-only input tables do not.
- The table you pass to `delete` must contain only key columns, not value columns.

## Standard and append-only tables

**Standard and append-only tables don't support direct row deletion.** There's no API to delete individual rows from these table types:

- **Append-only tables**: Rows can only be added to the end. Once added, rows cannot be modified, deleted, or reindexed.
- **Standard streaming tables**: Rows can be added, modified, removed, or reindexed by whatever updates the table (its data source or upstream operations), but there's no method to delete individual rows yourself.

For these table types, you can:

- Use [`where`](../table-operations/filter/where.md) to filter out unwanted rows (creates a new filtered view, doesn't delete data).
- Use [`head`](../table-operations/filter/head.md) or [`tail`](../table-operations/filter/tail.md) to limit the table size (creates a new view with limited rows).
- If the data arrives as a stream, ingest it as a [blink table](../../conceptual/table-types.md#specialization-3-blink) and build a [ring table](../../how-to-guides/ring-table.md) from it, so only the latest N rows are kept. A ring table built on an append-only table does not save memory, because the append-only table still keeps every row.

## Ring tables: automatic data disposal

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

## Blink tables: data automatically cleared each cycle

[Blink tables](../../conceptual/table-types.md#specialization-3-blink) automatically clear all data after each update cycle, retaining only the most recent batch of rows. This makes them excellent for low-memory streaming applications:

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.TimeTable.Builder

// Create a blink table that only keeps data from the current cycle
builder = new Builder().period("PT0.5s").blinkTable(true)
t = builder.build()
```

Blink tables don't require manual deletion — they automatically discard previous data with each update.

## Partitioned table partitions

For [partitioned tables](../../how-to-guides/partitioned-tables.md), you can exclude entire constituent tables (partitions) by creating a new, filtered partitioned table, but you cannot delete individual rows within a partition unless the underlying table type supports it (e.g., if the constituent tables are input tables).

To work with specific partitions:

- Use [`constituentFor`](../table-operations/partitioned-tables/constituentFor.md) to retrieve a specific partition by key.
- Create a new partitioned table excluding unwanted partitions using [`filter`](../table-operations/partitioned-tables/filter.md).

## Summary

| Table type                                      | Can delete data? | How?                                                                  |
| ----------------------------------------------- | ---------------- | --------------------------------------------------------------------- |
| **Keyed input tables**                          | ✅ Yes           | `delete` or `deleteAsync` methods                                     |
| **Append-only input tables**                    | ❌ No            | Not supported                                                         |
| **Standard and append-only (non-input) tables** | ❌ No            | Use filtering, or ingest as blink + ring instead                      |
| **Ring tables**                                 | ✅ Automatic     | Old data discarded automatically when capacity is reached             |
| **Blink tables**                                | ✅ Automatic     | Data cleared after each update cycle                                  |
| **Partitioned tables**                          | ⚠️ Partial        | Can filter partitions; row deletion depends on constituent table type |

## Related documentation

- [Create and use input tables](../../how-to-guides/input-tables.md)
- [Table types](../../conceptual/table-types.md)
- [Create and use ring tables](../../how-to-guides/ring-table.md)
- [Create and use partitioned tables](../../how-to-guides/partitioned-tables.md)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
