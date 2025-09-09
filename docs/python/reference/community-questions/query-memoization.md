---
title: Why do some queries run faster the second time they are run?
sidebar_label: Why do queries run faster the second time they are run?
---

Deephaven utilizes [memoization](https://en.wikipedia.org/wiki/Memoization) for many table operations, such as [`avg_by`](../table-operations/group-and-aggregate/avgBy.md), which stores and recalls previously computed results. This optimization significantly reduces execution time on repeated runs.

## Are all operations in Deephaven memoized?

Not all operations are memoized. Some operations are excluded for one of two reasons:

- The work hasn't been done to memoize the table operation (yet).
- Memoization is unsafe, especially with stateful User-Defined Functions (UDFs).

## Are there other factors contributing to improved execution time on subsequent runs?

Yes. Other factors include the following, which is often done on the first pass:

- Warmed filesystem and/or operating system buffer caches, which result in a complete in-memory cache of materialized pages.
- JIT compilation, which optimizes execution for subsequent runs.

## Can memoization be disabled for benchmark testing?

Yes, if you're benchmarking and want to disable memoization, you can do so. Set the property `QueryTable.memoizeResults` to `false`.

You can include this setting in your .prop file:

```bash
QueryTable.memoizeResults=false
```

Or as a JVM argument:

```bash
docker run --rm --env START_OPTS="-DQueryTable.memoizeResults=false" ghcr.io/deephaven/server:latest
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
