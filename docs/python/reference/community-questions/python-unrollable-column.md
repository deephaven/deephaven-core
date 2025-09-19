---
title: How do I create unrollable columns in the Python IDE?
sidebar_label: How do I create unrollable columns in the Python IDE?
---

Grouping and ungrouping (also referred to as "rolling up" or "unrolling") column data is a hugely important part of staying organized when working with Deephaven. However, since Deephaven's engine is built in Java, the [`ungroup`](../table-operations/group-and-aggregate/ungroup.md) method only works on Java arrays - for example, the following query will fail:

```python skip-test
offsets = [1, 2, 3, 4]
table = tabe.update("offsets = offsets")
table.ungroup("offsets")
```

The offsets need to be converted to a Java array for the `ungroup` method to work:

```python order=null
from deephaven import empty_table
from deephaven import dtypes

offsets = [1, 2, 3, 4]
offsets = dtypes.array(dtypes.int64, offsets)

t1 = empty_table(10).update("X = i")
t2 = t1.update("Offsets=offsets")
t3 = t2.ungroup()
t4 = t2.ungroup("Offsets")
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
