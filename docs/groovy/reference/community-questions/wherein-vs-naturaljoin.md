---
title: When should I use `whereIn` or `naturalJoin`?
---

_When should I use [`whereIn`](../table-operations/filter/where-in.md) versus [`naturalJoin`](../table-operations/join/natural-join.md) in my queries?_

Generally, it's best to use [`whereIn`](../table-operations/filter/where-in.md) when your list of values (your right table) stays relatively stable. This is because the [`whereIn`](../table-operations/filter/where-in.md) call maintains a minimal amount of state compared to [`naturalJoin`](../table-operations/join/natural-join.md). With [`whereIn`](../table-operations/filter/where-in.md), the set of unique values in the right table is maintained in memory. Only the Index of rows that currently match the [`whereIn`](../table-operations/filter/where-in.md) filter is maintained for the left table. When the valid set changes, the columns that you are using to filter the left table must be scanned. When the right table changes infrequently, this is a useful trade-off. The left table is seldom scanned, and less memory and computation are used on each update.

[`naturalJoin`](../table-operations/join/natural-join.md) maintains a state entry for _all_ values in both the left and right tables. This includes all keys that currently pass the filter, but also all keys that do not pass the filter. For each key, an Index data structure is maintained for the left and right side. When a value enters the right table, all of the matching left-hand side rows are available and can be augmented with the corresponding right-hand side row. When a value leaves the right table, all of the matching left-hand side rows are available, and the augmentation can be removed. This means that processing each individual left-hand side update is more expensive, but the left-hand table doesn't have to be scanned when the right-hand side table changes.

Traders or firms often have a fairly stable set of symbols in which they have a position. Using a [`whereIn`](../table-operations/filter/where-in.md) clause to filter other tables to those positions is generally efficient. The query doesn't have to maintain excess state for the rare cases where valid symbols change. However, full table scans are required when the positions _do_ change.

On the other hand, the set of open order IDs changes frequently. In this case, filtering a table by open order ID using [`whereIn`](../table-operations/filter/where-in.md) would require frequent scans. A [`naturalJoin`](../table-operations/join/natural-join.md) would require more in-memory state, but would be able to compute the results more incrementally.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
