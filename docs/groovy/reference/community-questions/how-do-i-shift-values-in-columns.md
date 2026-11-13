---
title: How do I shift values in columns?
---

Use the special variables [`i` or `ii`](../query-language/variables/special-variables.md) with the [`_` array suffix and a constant offset](../query-language/types/arrays.md), such as `Col_[ii - 1]` (previous row) or `Col_[ii + 1]` (next row). The engine handles this simple constant-offset form on both [static](../../conceptual/table-types.md) and [ticking](../../conceptual/table-types.md#standard-streaming-tables) tables. More complex index expressions, such as `Col_[(ii) - 1]`, are only safe on static and blink tables; for those on refreshing tables, see [Alternatives for refreshing tables](../../how-to-guides/built-in-variables.md#alternatives-for-refreshing-tables).

For example:

```groovy order=source,result
source = emptyTable(5).update("Col = ii")
result = source.update("ColPrev = Col_[ii - 1]", "ColNext = Col_[ii + 1]")
```

The first row of `ColPrev` and the last row of `ColNext` are null because there is no row before the first or after the last.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
