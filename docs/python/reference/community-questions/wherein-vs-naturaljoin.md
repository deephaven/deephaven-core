---
title: When should I use `where_in` or `natural_join`?
---

_When should I use [`where_in`](../table-operations/filter/where-in.md) versus [`natural_join`](../table-operations/join/natural-join.md) in my queries?_

Generally, it's best to use `where_in` when your list of values stays relatively stable. In both operations, the table that holds that list of values is the right table, and the table you filter or join onto is the left table. `where_in` maintains less state than `natural_join`: it keeps the set of unique values in the right table in memory, and only the set of rows (the RowSet) that currently match the filter for the left table.

When the set of values in the right table changes, Deephaven re-evaluates left-table rows against the new set: rows that don't match yet when keys are added, and rows that currently match when keys are removed. If the left table has a [data index](../../how-to-guides/data-indexes.md) on the key columns, Deephaven can use the index to accelerate re-evaluation. When the right table changes infrequently, this is a useful trade-off — the left table is seldom re-evaluated, and less memory and computation are used on each update.

`natural_join` maintains a state entry for _all_ values in both the left and right tables, including keys that don't currently match. For each key, it tracks the set of matching left-table rows and the matching right-table row. When a value enters the right table, all matching left-table rows are already known and get the right-table row's columns added to them. When a value leaves the right table, those added values are removed. Processing each individual left-table update is more expensive, but the left table doesn't have to be re-evaluated when the right table changes.

Traders or firms often have a fairly stable set of symbols in which they have a position. Using `where_in` to filter other tables to those positions is generally efficient. The query doesn't have to maintain excess state for the rare cases where the set of position symbols changes. However, the left table has to be re-evaluated when the positions _do_ change.

On the other hand, the set of open order IDs changes frequently. In this case, filtering a table by open order ID using `where_in` would require frequent re-evaluations. A `natural_join` would require more in-memory state, but would be able to compute the results more incrementally. To filter with `natural_join`, the right table needs one row per key (use [`select_distinct`](../table-operations/select/select-distinct.md) or [`last_by`](../table-operations/group-and-aggregate/lastBy.md) on the key column if it has duplicates, or set `type` to a duplicate-tolerant policy like `NaturalJoinType.FIRST_MATCH`). Join a marker column from the right table, then keep the rows where that column isn't null.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
