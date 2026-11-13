---
title: where
---

<!-- Excerpt of docs/python/reference/table-operations/filter/where.md at da80684afe (PR #7457), a Reference page. Only the section Chip Kent commented on is included; the rest of the page is omitted. Relative links resolve from docs/python/reference/table-operations/filter/. -->

## Filters on partitioning columns

When a table comes from a partitioned source, such as a directory of Parquet files or an Iceberg table, a filter that uses only partitioning columns can be applied to the partitions before any data is read. Deephaven evaluates the filter once per partition instead of once per row, and runs it ahead of the other filters, so whole partitions are skipped.

Deephaven applies a filter this way even when filters are configured to be stateful by default, because that is nearly always what users want. For example, `Date = today()` is stateful when filters are stateful by default, but Deephaven still evaluates it early, partition by partition.

A filter on partitioning columns is not applied this way if:

- It is marked serial with [`with_serial`](../../query-language/types/Filter.md#with_serial), or any filter before it in the argument list is. From the first serial filter on, Deephaven evaluates that filter and every later one on the table's rows instead of on whole partitions.
- It respects a Barrier declared by a filter that is not applied this way.
- It uses row variables such as `i` or `ii`, or its results can change over time (a refreshing filter).

Mark a filter on partitioning columns serial only when the order in which it is evaluated matters.

## Related documentation

- [Filter](../../query-language/types/Filter.md)
