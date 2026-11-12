---
title: How do I create a partitioned blink table?
---

<em>I can create a partitioned blink table, but why do I have to drop the blink attribute and re-add it to do so?</em>

<p></p>

Aggregations on blink tables are defined to act as if you had full history from the time of operation instantiation. For example, if you use [`last_by`](../table-operations/group-and-aggregate/lastBy.md) on a [blink table](../../conceptual/table-types.md#specialization-3-blink), you get the last row for each bucket, considering all rows that have ever been processed by the operation since it was created — even if those rows are no longer part of the current update cycle. Deephaven does not allow users to perform operations that can't deliver those semantics — meaning [`group_by`](../table-operations/group-and-aggregate/groupBy.md), [`partition_by`](../table-operations/group-and-aggregate/partitionBy.md), and [rollup tables](../table-operations/create/rollup.md) with constituents will not work.

To get around these restrictions, you can use [`remove_blink`](../table-operations/create/remove-blink.md) to opt out of special semantics. Then, you can use [`partition_by`](../table-operations/group-and-aggregate/partitionBy.md) to get a result with constituents that will blink in and out of existence. Finally, use `transform` to add the blink attribute back to each constituent so that operations on a constituent (for example, aggregations) keep blink-table semantics.

```python ticking-table order=null
from deephaven import time_table
import jpy

j_table = jpy.get_type("io.deephaven.engine.table.Table")

t_blink = time_table("PT0.1s", blink_table=True).update(
    ["X = ii", "Group = ii % 2 == 0 ? `A` : `B`"]
)

partitioned_blink_table = (
    t_blink.remove_blink()
    .partition_by("Group")
    .transform(lambda t: t.with_attributes({j_table.BLINK_TABLE_ATTRIBUTE: True}))
)
```

![A ticking blink table with X and Group columns](../../assets/reference/community-questions/t_blink.png)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
