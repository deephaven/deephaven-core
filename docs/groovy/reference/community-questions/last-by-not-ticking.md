---
title: Why isn't `lastBy` updating when called on ticking data?
sidebar_label: Why isn't `lastBy` ticking?
---

_I'm calling [`lastBy`](../table-operations/group-and-aggregate/lastBy.md) on a ticking table. The resultant table isn't ticking with the upstream ticking table. What's going on?_

The [`lastBy`](../table-operations/group-and-aggregate/lastBy.md) operation returns the last row for each group as specified by the given key column(s). Oftentimes, queries on ticking data call [`reverse`](../table-operations/sort/reverse.md) on ticking tables so that new rows appear at the top. This enables users to see new data arrive without having to scroll down to the bottom. The problem with using [`lastBy`](../table-operations/group-and-aggregate/lastBy.md) in this case is that no new data arrives at the bottom of the table, so the result appears to be static.

If you [`reverse`](../table-operations/sort/reverse.md) a ticking table, use [`firstBy`](../table-operations/group-and-aggregate/firstBy.md) instead. Otherwise, don't reverse a ticking table before calling [`lastBy`](../table-operations/group-and-aggregate/lastBy.md).

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help.
