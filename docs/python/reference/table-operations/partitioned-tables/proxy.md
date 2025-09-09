---
title: proxy
---

The `proxy` method creates a new `PartitionedTableProxy` from the provided `PartitionedTable`.

A `PartitionedTableProxy` is a table operation proxy object for the underlying `PartitionedTable`. The `PartitionedTableProxy` gives users access to a variety of Deephaven table operations that are not available to a `PartitionedTable`. When a user has made all the desired changes to the `PartitionedTableProxy`, they can use the `target` attribute to return the underlying `PartitionedTable` with the changes applied.

A query can achieve the same results by using either [`transform`](./transform.md) or a partitioned table proxy. Proxy objects provide convenience by making it simpler to apply transformations to a partitioned table, but with less control.

## Syntax

```python syntax
PartitionedTable.proxy(
  require_matching_keys: bool = True,
  sanity_check_joins: bool = True
) -> PartitionedTableProxy
```

## Parameters

<ParamTable>
<Param name="require_matching_keys" type="bool">

Whether to ensure that both `PartitionedTable`s have all the same keys present when an operation uses this `PartitionedTable` and another `PartitionedTable` as inputs for a `partitioned_transform()`. The default is `True`.

</Param>
<Param name="sanity_check_joins" type="bool">

Whether to check that for proxied join operations, a given join key only occurs in exactly one constituent table of the underlying `PartitionedTable`. If the other table argument is also a `PartitionedTableProxy`, its constituents will be subjected to this sanity check.

</Param>
</ParamTable>

## Returns

A `PartitionedTableProxy`.

## Available methods

The following methods are available to a `PartitionedTableProxy`:

> [!NOTE]
> The following links are for the `Table` version of each of these methods. The `PartitionedTableProxy` version of each method is identical, except that it returns a `PartitionedTableProxy` instead of a `Table`.

- [`abs_sum_by()`](../group-and-aggregate/AbsSumBy.md)
- [`agg_all_by()`](../group-and-aggregate/AggAllBy.md)
- [`agg_by()`](../group-and-aggregate/aggBy.md)
- [`aj()`](../join/aj.md)
- [`avg_by()`](../group-and-aggregate/avgBy.md)
- [`count_by()`](../group-and-aggregate/countBy.md)
- [`exact_join()`](../join/exact-join.md)
- [`first_by()`](../group-and-aggregate/firstBy.md)
- [`group_by()`](../group-and-aggregate/groupBy.md)
- [`head()`](../filter/head.md)
- [`is_refreshing`](../metadata/is_refreshing.md)
- [`join()`](../join/join.md)
- [`last_by()`](../group-and-aggregate/lastBy.md)
- [`max_by()`](../group-and-aggregate/maxBy.md)
- [`median_by()`](../group-and-aggregate/medianBy.md)
- [`min_by()`](../group-and-aggregate/minBy.md)
- [`natural_join()`](../join/natural-join.md)
- [`raj()`](../join/raj.md)
- [`reverse()`](../sort/reverse.md)
- [`select()`](../select/select.md)
- [`select_distinct()`](../select/select-distinct.md)
- [`snapshot()`](../snapshot/snapshot.md)
- [`snapshot_when()`](../snapshot/snapshot-when.md)
- [`sort()`](../sort/sort.md)
- [`sort_descending()`](../sort/sort-descending.md)
- [`std_by()`](../group-and-aggregate/stdBy.md)
- [`sum_by()`](../group-and-aggregate/sumBy.md)
- [`tail()`](../filter/tail.md)
- [`update()`](../select/update.md)
- [`update_by()`](../update-by-operations/updateBy.md)
- [`update_graph`](../metadata/update_graph.md)
- [`update_view()`](../select/update-view.md)
- [`var_by()`](../group-and-aggregate/varBy.md)
- [`view()`](../select/view.md)
- [`weighted_avg_by()`](../group-and-aggregate/weighted-avg-by.md)
- [`weighted_sum_by()`](../group-and-aggregate/weighted-sum-by.md)
- [`where()`](../filter/where.md)
- [`where_in()`](../filter/where-in.md)
- [`where_not_in()`](../filter/where-not-in.md)

## Example

The following example shows how a partitioned table proxy can be used to apply standard table operations to every constituent of a partitioned table. The example uses [`reverse`](../sort/reverse.md) to reverse the ordering of the original table.

```python order=result_from_proxy,result,source
from deephaven import empty_table

source = empty_table(10).update(["Key = (i % 2 == 0) ? `X` : `Y`", "Value = i"])
partitioned_table = source.partition_by(["Key"])

pt_proxy = partitioned_table.proxy()

result = source.reverse()
proxy_reversed = pt_proxy.reverse()
result_from_proxy = proxy_reversed.target.merge()
```

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTable.proxy)
