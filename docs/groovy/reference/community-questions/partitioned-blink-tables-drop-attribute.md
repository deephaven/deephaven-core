---
title: Why do I have to drop the blink attribute and re-add it to create a partitioned blink table?
sidebar_label: How do I create a partitioned blink table?
---

<em>I can create a partitioned blink table, but why do I have to drop the blink attribute and re-add it to do so?</em>

<p></p>

Aggregations on blink tables are defined to act as if you had full history from the time of operation instantiation. For example, if you use [`lastBy`](../table-operations/group-and-aggregate/lastBy.md) on a [blink table](../../conceptual/table-types.md#specialization-3-blink), you get the last `n` rows for each bucket, considering all rows that have ever been processed by the operation since it was created - even if those rows are no longer part of the current update cycle. Deephaven does not allow users to perform operations that can't deliver those semantics - meaning [`groupBy`](../table-operations/group-and-aggregate/groupBy.md), [`partitionBy`](../table-operations/group-and-aggregate/partitionBy.md), and [rollup tables](../table-operations/create/rollup.md) with constituents will not work.

To get around these restrictions, you can use [`removeBlink`](../table-operations/create/remove-blink.md) to opt out of special semantics. Then, you can use [`partitionBy`](../table-operations/group-and-aggregate/partitionBy.md) to get a result with constituents that will blink in and out of existence.

```groovy order=partitionedBlinkTable
import io.deephaven.engine.table.impl.TimeTable.Builder
import io.deephaven.engine.table.Table

builder = new Builder().period("PT0.1s").blinkTable(true)

tBlink = builder.build().update("X = ii", "Group = ii % 2 == 0 ? `A` : `B`")

partitionedBlinkTable = tBlink
    .removeBlink()
    .partitionBy("Group")
    .transform(t -> t.withAttributes(Map.of(Table.BLINK_TABLE_ATTRIBUTE, true)))
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
