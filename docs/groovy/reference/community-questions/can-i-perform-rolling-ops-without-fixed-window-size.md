---
title: How can I perform rolling operations without a fixed window size?
---

<em>I have a table of data where I'd like to calculate some rolling metrics. However, instead of a fixed window size, I'd like the rolling metrics to be based on values in another column. When I see a particular value in the other column, I want the metrics to reset. How can I do this?</em>

<p></p>

You can accomplish this with an [`updateView`](../table-operations/select/update-view.md) and two [`updateBy`](../table-operations/update-by-operations/updateBy.md) operations. Say, for instance, you have a `source` table with two columns: `Value` and `Sym`. You want to calculate the running sum of `Value`, but have that sum reset every time `DesiredValue` occurs in `Sym`. The first `updateBy` numbers each segment with a cumulative sum of the reset marker, and the second sums `Value` within each segment:

```groovy order=source,result
source = newTable(
    stringCol("Sym", "A", "A", "DesiredValue", "B", "B", "DesiredValue", "C"),
    intCol("Value", 1, 2, 3, 4, 5, 6, 7),
)

result = source.updateView("IsReset = Sym == `DesiredValue` ? 1 : 0")
    .updateBy(CumSum("GroupId = IsReset"))
    .updateBy(CumSum("SumValue = Value"), "GroupId")
```

This uses [`CumSum`](../table-operations/update-by-operations/cum-sum.md); the same group-ID column works with any windowed `Rolling*` operation.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
