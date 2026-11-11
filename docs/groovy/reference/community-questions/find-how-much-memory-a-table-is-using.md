---
title: Is there a way to find out how much memory a table is using?
---

No. You can look at overall memory usage in the UI, but there's not a way to look at memory usage by a single table. You can approximate how much memory it uses based on the table's size and data types. Tables also use memory for their RowSet — the structure that tracks which row keys the table contains — and for any state that operations such as joins and aggregations keep.

We have some general advice, however:

- Review your query to see if it is using "expensive" operations.
  - [`update`](../table-operations/select/update.md) stores every new column in memory. A billion-row table with one new `long` column needs at least 1 billion \* 8 bytes.
  - [Joins](../../how-to-guides/joins-exact-relational.md) can be expensive when there are many unique join keys.
  - Joins and [`sort`](../table-operations/sort/sort.md) can be expensive on very large tables, depending on where the rows come from and how they are ordered.
  - Anything that creates many small row sets, such as [`groupBy`](../table-operations/group-and-aggregate/groupBy.md) or [`partitionBy`](../table-operations/group-and-aggregate/partitionBy.md) (one per group or partition), can also use a lot of memory.
- Look at memory usage in the [Query Operation Performance Log (QOPL)](../../how-to-guides/performance/performance-tables.md#query-operation-performance-log).
  - This works best when the query starts from tables that already hold a lot of data (e.g., a million-row table rather than an empty table that fills as data from a Kafka stream is ingested). Look at the `FreeMemoryChange` and `AllocatedBytes` columns in the QOPL to see which operations allocated the most memory. These are JVM-wide measurements, so other activity and garbage collection can affect the results, but an expensive `update` or join is often readily apparent.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
