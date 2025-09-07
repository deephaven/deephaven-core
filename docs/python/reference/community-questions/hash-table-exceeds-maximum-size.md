---
title: How do I solve a `hash table exceeds maximum size` error?
---

<em>I am getting a `hash table exceeds maximum size` error when trying to perform simple operations on huge tables. What's going on?</em>

<p></p>

If you see a `hash table exceeds maximum size` error, it means that your table has too many keys for Deephaven to handle - without reducing cardinality. Fortunately, this is easy to do.

To get around this problem, simply use [`partition_by`](../../reference/table-operations/group-and-aggregate/partitionBy.md) to create subtables with a lesser key cardinality. Take this example with 100 million rows:

```python skip-test
from deephaven import empty_table

t = empty_table(100_000_000).update("key = randomInt(0, 10000000)")

# will cause a "hash table exceeds maximum size" error on very large tables
rst = t.count_by("Count", "key")
```

This can be reformulated using partitioned tables into a form that has lower key cardinality that will avoid the error:

```python skip-test
from deephaven import empty_table

t = empty_table(100_000_000).update("key = randomInt(0, 10000000)")

# partition the table to reduce key cardinality
rst = (
    t.update_view("Partition = key % 100")
    .partition_by(["Partition"])
    .proxy()
    .count_by("Count", "key")
    .target.merge()
)
```

Additionally, working with large data amplifies any inefficiency in your code, so make sure you're using the correct [join](../../how-to-guides/joins-exact-relational.md#which-method-should-you-use) and [selection](../../how-to-guides/use-select-view-update.md#choose-the-right-column-selection-method) methods for your use case.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
