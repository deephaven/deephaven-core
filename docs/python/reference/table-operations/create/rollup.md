---
title: rollup
---

The Deephaven `rollup` method creates a rollup table from a source table with zero or more aggregations and zero or more grouping columns to create a hierarchy.

## Syntax

```python syntax
result = source.rollup(
    aggs: list[Aggregation],
    by: list[str] = None,
    include_constituents: bool = False,
) -> RollupTable
```

## Parameters

<ParamTable>
<Param name="aggs" type="list[Aggregation]">

A list of aggregations. If `None`, no aggregations are performed.

The following aggregations are supported:

- [`abs_sum`](../group-and-aggregate/AggAbsSum.md)
- [`avg`](../group-and-aggregate/AggAvg.md)
- [`count_`](../group-and-aggregate/AggCount.md)
- [`count_distinct`](../group-and-aggregate/AggCountDistinct.md)
- [`count_where`](../group-and-aggregate/AggCountWhere.md)
- [`first`](../group-and-aggregate/AggFirst.md)
- [`last`](../group-and-aggregate/AggLast.md)
- [`max_`](../group-and-aggregate/AggMax.md)
- [`min_`](../group-and-aggregate/AggMin.md)
- [`sorted_first`](../group-and-aggregate/AggSortedFirst.md)
- [`sorted_last`](../group-and-aggregate/AggSortedLast.md)
- [`std`](../group-and-aggregate/AggStd.md)
- [`sum`](../group-and-aggregate/AggSum.md)
- [`unique`](../group-and-aggregate/AggUnique.md)
- [`var`](../group-and-aggregate/AggVar.md)
- [`weighted_avg`](../group-and-aggregate/AggWAvg.md)
- [`weighted_sum`](../group-and-aggregate/AggWSum.md)

</Param>
<Param name="by" type="list[str]">

Zero or more column names to group on and create a hierarchy from. If `None`, no hierarchy is created.

</Param>
<Param name="include_constituents" optional type="bool">

Whether or not to include constituent rows at the leaf level. Default is False.

</Param>
</ParamTable>

## Methods

### Instance

- `with_filters(filters...)` - Create a new rollup table that applies a set of filters to the `groupByColumns` of the rollup table.
- `with_update_view(columns...)` - Create a new rollup table that applies a set of `update_view` operations to the `groupByColumns` of the rollup table.
- `node_operation_recorder(nodeType)` - Get a [`recorder`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.RollupNodeOperationsRecorder) for per-node operations to apply during snapshots of the requested [`NodeType`](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.NodeType.html).
- `with_node_operations(recorders...)` - Create a new rollup table that applies the [`recorded`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.RollupNodeOperationsRecorder) operations to nodes when gathering snapshots.

## Returns

A rollup table.

## Examples

The following example creates two rollup tables from a source table of insurance expense data. The first performs no aggregations, but creates a hierarchy from the `region` and `age` columns. The second performs two aggregations: the aggregated average of the `bmi` and `expenses` columns are calculated, then the same `by` columns are given as the first. The optional argument `include_constituents` is set to `True` so that members of the lowest-level nodes (individual cells) can be expanded.

```python order=insurance,insurance_rollup
from deephaven import read_csv, agg

insurance = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv"
)

agg_list = [agg.avg(cols=["bmi", "expenses"])]
by_list = ["region", "age"]

insurance_rollup = insurance.rollup(
    aggs=agg_list, by=by_list, include_constituents=False
)
```

Similar to the previous example, this example creates a rollup table from a source table of insurance expense data. However, this time we are filtering on the source table before applying the rollup using `with_filters`. Both group and constituent columns can be used in the filter, while aggregation columns cannot.

```python order=insurance,insurance_rollup
from deephaven import read_csv, agg

insurance = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv"
)

agg_list = [agg.avg(cols=["bmi", "expenses"])]
by_list = ["region", "age"]
filter_list = ["age > 30"]

insurance_rollup = insurance.rollup(
    aggs=agg_list, by=by_list, include_constituents=False
).with_filters(filter_list)
```

The following example creates a rollup table from real-time source data. The source data updates 10,000 times per second. The `result` rollup table can be expanded by the `N` column to show unique values of `M` for each `N`. The aggregated average and sum are calculated for the `Value` and `Weight`, respectively.

```python ticking-table order=null
from deephaven import empty_table, time_table
from deephaven import agg

agg_list = [agg.avg(cols=["Value"]), agg.sum_(cols=["Weight"])]
by_list = ["N", "M"]

rows = empty_table(1_000_000).update_view(["Group = i", "N = i % 347", "M = i % 29"])
changes = (
    time_table("PT0.0001S")
    .view(
        [
            "Group = i % 1_000_000",
            "LastModified = Timestamp",
            "Value = (i * Math.sin(i)) % 6977",
            "Weight = (i * Math.sin(i)) % 7151",
        ]
    )
    .last_by("Group")
)

source = rows.join(changes, "Group")

result = source.rollup(aggs=agg_list, by=by_list)
```

![The above `result` rollup table](../../../assets/how-to/rollup-table-realtime.gif)

## Related documentation

- [`avg`](../group-and-aggregate/AggAvg.md)
- [`sum`](../group-and-aggregate/AggSum.md)
- [`empty_table`](./emptyTable.md)
- [`join`](../join/join.md)
- [`time_table`](./timeTable.md)
- [`tree`](./tree.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.rollup)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/hierarchical/RollupTable.html)
