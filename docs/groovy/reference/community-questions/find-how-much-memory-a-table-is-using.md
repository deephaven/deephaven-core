---
title: Is there a way to find out how much memory a table is using?
sidebar_label: Is there a way to find out how much memory a table is using?
---

No. You can look at overall memory usage in the UI, but there's not a way to look at memory usage by a single table. You can approximate how much memory it uses based on the table's size and data types. Tables use additional memory for the row set and column sources.

We have some general advice, however:

- Review your query to see if it is using "expensive" operations.
  - As a general rule, if a query is devouring memory, it is probably using an `update()`, `sort()`, or join. `update()` is easiest to think about ("the table has a billion rows, I do `update()` for a column of longs, that's at least 1 billion \* 8 bytes"); join and sort are harder to think about, but in general joins can be expensive if you have tons of unique join keys, and both join and sort can be bad if you have tons of rows (but it depends quite a bit on where the rows come from and how they're ordered).
  - Anything that creates a lot of small indices, like `group_by` or `partition_by`, may also consume a lot of memory.
- Look at memory usage in the [Query Operation Performance Log](../../how-to-guides/performance-tables.md#query-operation-performance-log).
  - If you initialize a query against tables that already have lots of data in them (i.e., start from a million-row table, not an empty table that will be populated as data from a Kafka stream is ingested), you can look at the QOPL to see the operations during which the query started using more memory. Note that external operations / Garbage Collection can affect the results, but often enough an expensive update or join or will be readiliy apparent.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
