---
title: Why do some queries run faster the second time they are run?
---

Deephaven uses [memoization](https://en.wikipedia.org/wiki/Memoization) for many table operations, such as [`avg_by`](../table-operations/group-and-aggregate/avgBy.md). Memoization stores previously computed results and reuses them instead of computing them again. When you apply the same operation with the same arguments to the same table again, and the earlier result still exists, Deephaven returns that existing result instead of computing it again.

## Are all operations in Deephaven memoized?

Not all operations are memoized. Some operations are excluded for one of two reasons:

- The work hasn't been done to memoize the table operation (yet).
- Memoizing the operation would not be safe. For example, `select`, `update`, and `view` calls that compute formula columns are never memoized, and neither are most formula filters, because a formula can return a different result each time it runs.

## Do other factors besides memoization make later runs faster?

Yes. The first run also warms caches and compiles code, and later runs benefit from both:

- Warm filesystem and operating system caches. Data read from disk on the first run stays in memory, so later runs read it from memory instead of from disk.
- Just-in-time (JIT) compilation. The JVM compiles frequently run code to optimized machine code while the first run executes, so later runs use the faster compiled code.

## Can memoization be disabled for benchmark testing?

Yes. To disable memoization when benchmarking, set the property `QueryTable.memoizeResults` to `false`. See [Query table configuration](../../conceptual/query-table-configuration.md#memoization) for details.

You can include this setting in your [configuration file](../../how-to-guides/configuration/config-file.md):

```properties
QueryTable.memoizeResults=false
```

Or pass it as a JVM system property. With Docker, set it in the `START_OPTS` environment variable:

```bash
docker run --rm --env START_OPTS="-DQueryTable.memoizeResults=false" ghcr.io/deephaven/server:latest
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
