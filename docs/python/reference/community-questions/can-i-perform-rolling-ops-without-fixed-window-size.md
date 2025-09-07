---
title: How can I perform rolling operations without a fixed window size?
sidebar_label: How can I perform rolling operations without a fixed window size?
---

<em>I have a table of data where I'd like to calculate some rolling metrics. However, instead of a fixed window size, I'd like the rolling metrics to be based on values in another column. When I see a particular value in the other column, I want the metrics to reset. How can I do this?</em>

<p></p>

You can accomplish this with an [`update`](../table-operations/select/update.md) or an [`update_view`](../table-operations/select/update-view.md) and two [`update_by`](../table-operations/update-by-operations/updateBy.md) operations. Say, for instance, you have a `source` table with two columns: `Value` and `Sym`. You want to calculate the running sum of `Value`, but have that sum reset every time `DesiredValue` occurs in `Sym`. A solution would look something like this:

```python skip-test
result = source.update_view(["Counter = Sym == `DesiredValue`"]).update_by(
    ops=cum_sum(["SumValue = Value"], by=["Counter"])
)
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
