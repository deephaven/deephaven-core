---
title: How do I create unrollable columns in the Python IDE?
---

Ungrouping (sometimes called unrolling) a column of arrays expands each element into its own row. Deephaven's engine is built in Java, so the [`ungroup`](../table-operations/group-and-aggregate/ungroup.md) method works only on Java array and Deephaven Vector columns, not on Python lists. For example, the following query fails:

```python skip-test
from deephaven import empty_table

offsets = [1, 2, 3, 4]
t = empty_table(10).update("Offsets = offsets")
t.ungroup("Offsets")
```

Convert the list to a Java array with [`dtypes.array`](../../how-to-guides/work-with-arrays.md) before you add it to a table:

```python order=null
from deephaven import empty_table
from deephaven import dtypes

offsets = [1, 2, 3, 4]
offsets = dtypes.array(dtypes.int64, offsets)

t1 = empty_table(10).update("X = i")
t2 = t1.update("Offsets = offsets")
t3 = t2.ungroup("Offsets")
```

Calling `ungroup` with no arguments ungroups every array or Vector column in the table. Calling `ungroup("Offsets")` ungroups only the named column. In this example, because `Offsets` is the only array column, the results are the same. Each of the 10 rows becomes 4 rows, one per offset.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
