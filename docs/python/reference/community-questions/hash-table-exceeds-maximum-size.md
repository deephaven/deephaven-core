---
title: How do I solve a `hash table exceeds maximum size` error?
---

<em>I am getting an `UnsupportedOperationException: Hash table exceeds maximum size!` error when trying to perform key-based operations on tables with many distinct keys. What's going on?</em>

<p></p>

This error means a key-based operation — such as an aggregation, natural join, or as-of join — has more distinct key values than one hash table can hold (roughly 800 million). To avoid it, split the work so each hash table sees fewer keys.

Use [`partition_by`](../../reference/table-operations/group-and-aggregate/partitionBy.md) to create subtables with lower key cardinality. The following example shows the pattern, though its key range (10 million) is too small to trigger the error on its own:

```python skip-test
from deephaven import empty_table

t = empty_table(100_000_000).update("Key = randomInt(0, 10000000)")

# With enough distinct key values (roughly 800 million or more), this raises the error
rst = t.count_by("Count", "Key")
```

This can be reformulated using partitioned tables to avoid the error:

```python skip-test
from deephaven import empty_table

t = empty_table(100_000_000).update("Key = randomInt(0, 10000000)")

# Partition the table to reduce key cardinality
rst = (
    t.update_view("Partition = Key % 100")
    .partition_by(["Partition"])
    .proxy()
    .count_by("Count", "Key")
    .target.merge()
)
```

Each of the 100 constituent tables runs its own `count_by` on about 1/100 of the keys, and because each key falls in exactly one partition, `merge` returns the same counts as a single `count_by`, though rows may appear in a different order. For joins, partition both tables on the same derived column before performing the proxy join. See the [partitioned tables guide](../../how-to-guides/partitioned-tables.md) for more detail.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
