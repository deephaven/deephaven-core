---
title: How can I perform rolling operations without a fixed window size?
---

<em>I have a table of data where I'd like to calculate some rolling metrics. However, instead of a fixed window size, I'd like the rolling metrics to be based on values in another column. When I see a particular value in the other column, I want the metrics to reset. How can I do this?</em>

<p></p>

You can accomplish this with an [`update_view`](../table-operations/select/update-view.md) and two [`update_by`](../table-operations/update-by-operations/updateBy.md) operations. Say, for instance, you have a `source` table with two columns: `Value` and `Sym`. You want to calculate the running sum of `Value`, but have that sum reset every time `DesiredValue` occurs in `Sym`. The first `update_by` numbers each segment with a cumulative sum of the reset marker, and the second sums `Value` within each segment:

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.updateby import cum_sum

source = new_table(
    [
        string_col("Sym", ["A", "A", "DesiredValue", "B", "B", "DesiredValue", "C"]),
        int_col("Value", [1, 2, 3, 4, 5, 6, 7]),
    ]
)

result = (
    source.update_view(["IsReset = Sym == `DesiredValue` ? 1 : 0"])
    .update_by(ops=cum_sum(["GroupId = IsReset"]))
    .update_by(ops=cum_sum(["SumValue = Value"]), by=["GroupId"])
)
```

This uses [`cum_sum`](../table-operations/update-by-operations/cum-sum.md); the same group-ID column works with any windowed `rolling_*` operation.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
