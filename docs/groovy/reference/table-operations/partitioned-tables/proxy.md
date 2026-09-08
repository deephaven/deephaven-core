---
title: proxy
---

The [`proxy`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#proxy()) method creates a `PartitionedTable.Proxy` that allows table operations to be applied to the constituent tables of the source `PartitionedTable`.

Each operation thus applied will produce a new `PartitionedTable` with the results, as in `transform(UnaryOperator, Dependency...)` or [`partitionedTransform(PartitionedTable, BinaryOperator, Dependency...)`](./partitionedTransform.md), and return a new proxy to that `PartitionedTable`.

> [!NOTE]
> If the `proxy` overload with no parameters is used, the result is the same as `proxy(true, false)`.

## Syntax

```
proxy()
proxy(requireMatchingKeys, sanityCheckJoinOperations)
```

## Parameters

<ParamTable>
<Param name="requireMatchingKeys" type="boolean">

Whether to ensure that both partitioned tables have all the same keys present when a proxied operation uses `this` and another `PartitionedTable` as inputs for a partitioned transform.

</Param>
<Param name="sanityCheckJoinOperations" type="boolean">

Whether to check that proxied join operations will only find a given join key in one constituent table for `this` and the table argument if it is also a proxy.

</Param>
</ParamTable>

> [!CAUTION]
> `PartitionedTable` transforms and proxies produce different results than on a single-table join (e.g., `naturalJoin`), `whereIn`, or `whereNotIn` when the filter or join keys span partitions. You must ensure that your data's keys map to appropriate partitions to enable correct answers.
>
> When `sanityCheckJoins` to the `proxy` method is true, the engine validates that join keys exist only in a single partition, but it does not validate that a key exists in the same partition in both the left and right table.

## Returns

A `PartitionedTable.Proxy` that allows table operations to be applied to the constituent tables of the source `PartitionedTable`.

## Examples

```groovy order=source,resultFromProxy
source = emptyTable(10).update('Key = (i % 2 == 0) ? `X` : `Y`', 'Value = i')

partitionedTable = source.partitionBy('Key')

ptProxy = partitionedTable.proxy()

result = source.reverse()
proxyReversed = ptProxy.reverse()
resultFromProxy = proxyReversed.target.merge()
```

## Related documentation

- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#proxy(boolean,boolean))
