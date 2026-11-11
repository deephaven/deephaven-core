---
title: Why isn't `last_by` updating when called on ticking data?
---

_I'm calling [`last_by`](../table-operations/group-and-aggregate/lastBy.md) on a ticking table. The result table isn't ticking with the upstream ticking table. What's going on?_

The `last_by` operation returns the last row for each group as specified by the given key column(s). Queries on ticking data often call [`reverse`](../table-operations/sort/reverse.md) on ticking tables so that new rows appear at the top. This enables users to see new data arrive without having to scroll down to the bottom. After `reverse`, new rows have the lowest row keys, so `last_by` returns the oldest row in each group instead of the newest. In an append-only table that row never changes, so existing groups look static even though new rows keep arriving.

For example:

```python
from deephaven import time_table

t = time_table("PT1s").update("Key = ii % 3")

# Shows the OLDEST row per group (not what you want)
wrong = t.reverse().last_by("Key")

# Shows the NEWEST row per group
right = t.reverse().first_by("Key")
```

If you `reverse` a ticking table, use [`first_by`](../table-operations/group-and-aggregate/firstBy.md) instead of `last_by`. Alternatively, call `last_by` first and then `reverse` the result.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help.
