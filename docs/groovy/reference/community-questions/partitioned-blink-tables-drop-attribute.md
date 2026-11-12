---
title: Why do I have to drop the blink attribute and re-add it to create a partitioned blink table?
---

<em>I can create a partitioned blink table, but why do I have to drop the blink attribute and re-add it to do so?</em>

<p></p>

Aggregations on [blink tables](../../conceptual/table-types.md#specialization-3-blink) behave as though every row ever processed is still available. For example, [`lastBy`](../table-operations/group-and-aggregate/lastBy.md) returns the most recent `n` rows for each bucket across all update cycles, not just the current one. Some operations cannot deliver this guarantee. [`groupBy`](../table-operations/group-and-aggregate/groupBy.md), [`partitionBy`](../table-operations/group-and-aggregate/partitionBy.md), and [rollup tables](../table-operations/create/rollup.md) with constituent tables (the sub-tables inside a partitioned or rollup result) are therefore not allowed on blink tables.

To get around these restrictions, you can use [`removeBlink`](../table-operations/create/remove-blink.md) to opt out of the full-history aggregation behavior. Then, you can use [`partitionBy`](../table-operations/group-and-aggregate/partitionBy.md) to get a result with constituent tables that will blink in and out of existence.

```groovy order=null
import io.deephaven.engine.table.impl.TimeTable.Builder
import io.deephaven.engine.table.Table

builder = new Builder().period("PT0.1s").blinkTable(true)

tBlink = builder.build().update("X = ii", "Group = ii % 2 == 0 ? `A` : `B`")

partitionedBlinkTable = tBlink
    .removeBlink()
    .partitionBy("Group")
    .transform(t -> t.withAttributes(Map.of(Table.BLINK_TABLE_ATTRIBUTE, true)))
```

![t_blink](../../assets/reference/t_blink.png)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
