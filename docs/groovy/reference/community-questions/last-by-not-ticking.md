---
title: Why isn't `lastBy` updating when called on ticking data?
---

_I'm calling [`lastBy`](../table-operations/group-and-aggregate/lastBy.md) on a ticking table. The result table isn't ticking with the upstream table. What's going on?_

The [`lastBy`](../table-operations/group-and-aggregate/lastBy.md) operation returns the last row for each group as specified by the given key column(s). Queries on ticking data often call [`reverse`](../table-operations/sort/reverse.md) so that new rows appear at the top, where you can see them arrive without scrolling to the bottom. After `reverse`, the last row in each group is the oldest one, so new rows for existing keys never change the `lastBy` result. The result updates only when a row with a new key arrives or when the oldest rows change, making it appear static.

If you [`reverse`](../table-operations/sort/reverse.md) a ticking table, use [`firstBy`](../table-operations/group-and-aggregate/firstBy.md) instead. Alternatively, call `lastBy` first and reverse the result, or don't reverse the table at all.

```groovy ticking-table order=null
source = timeTable("PT1s").update("Key = ii % 3", "Value = ii")

// Use firstBy after reverse to get the newest row per key
reversedFirst = source.reverse().firstBy("Key")

// Or call lastBy first, then reverse the result
lastReversed = source.lastBy("Key").reverse()
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help.
