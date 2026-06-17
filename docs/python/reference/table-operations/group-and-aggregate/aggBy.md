---
title: agg_by
---

`agg_by` applies a list of aggregations to table data.

> [!WARNING]
> Aggregation keys consume memory that persists for the lifetime of the worker, even after the keys are removed from the table. Avoid including unnecessary columns in grouping keys, especially if they contain a high number of unique values.

## Syntax

```python syntax
agg_by(
    aggs: Union[Aggregation, Sequence[Aggregation]],
    by: Union[str, list[str]] = None,
    preserve_empty: bool = False,
    initial_groups: Table = None,
    ) -> Table
```

## Parameters

<ParamTable>
<Param name="aggs" type="Union[Aggregation, Sequence[Aggregation]]">

A list of aggregations to compute. The following aggregations are available:

- [`agg.abs_sum_by`](./AbsSumBy.md)
- [`agg.abs_sum`](./AggAbsSum.md)
- [`agg.agg_all_by`](./AggAllBy.md)
- [`agg.avg`](./AggAvg.md)
- [`agg.count_`](./AggCount.md)
- [`agg.count_distinct`](./AggCountDistinct.md)
- [`agg.count_where`](./AggCountWhere.md)
- [`agg.distinct`](./AggDistinct.md)
- [`agg.first`](./AggFirst.md)
- [`agg.formula`](./AggFormula.md)
- [`agg.group`](./AggGroup.md)
- [`agg.last`](./AggLast.md)
- [`agg.max_`](./AggMax.md)
- [`agg.median`](./AggMed.md)
- [`agg.min_`](./AggMin.md)
- [`agg.partition`](./AggPartition.md)
- [`agg.pct`](./AggPct.md)
- [`agg.sorted_first`](./AggSortedFirst.md)
- [`agg.sorted_last`](./AggSortedLast.md)
- [`agg.std`](./AggStd.md)
- [`agg.sum_`](./AggSum.md)
- [`agg.unique`](./AggUnique.md)
- [`agg.var`](./AggVar.md)
- [`agg.weighted_avg`](./AggWAvg.md)
- [`agg.weighted_sum`](./AggWSum.md)

</Param>
<Param name="by" type="Union[str, list[str]]">

The names of column(s) by which to group data. Default is `None`.

</Param>
<Param name="preserve_empty" type="bool" optional>

Whether to keep result rows for groups that are initially empty or become empty as a result of updates. Each aggregation operator defines its own value for empty groups. The default is `False`.

</Param>
<Param name="initial_groups" type="Table" optional>

A table whose distinct combinations of values for the grouping column(s) should be used to create an initial set of aggregation groups. All other columns are ignored.

- This is useful in combination with `preserve_empty=True` to ensure that particular groups appear in the result table, or with `preserve_empty=False` to control the encounter order for a collection of groups and thus their relative order in the result.
- Changes to this table are not expected or handled; if this table is a refreshing table, only its contents at instantiation time will be used.
- Default is `None`. The result will be the same as if a table is provided, but no rows were supplied. When it is provided, the ‘by’ argument must explicitly specify the grouping columns.

</Param>
</ParamTable>

> [!CAUTION]
> If an aggregation does not rename the resulting column, the aggregation column will appear in the output table, not the input column. If multiple aggregations on the same column do not rename the resulting columns, an error will result, because the aggregations are trying to create multiple columns with the same name. For example, in `table.agg_by([agg.sum_(“X”), agg.avg(“X”)])`, both the sum and the average aggregators produce column `X`, which results in an error.

## Returns

Aggregated table data based on the aggregation types specified in the `agg_list`.

## Examples

In this example, `agg.first` returns the first `Y` value as grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", "C"]),
        string_col("Y", ["M", "N", "O", "N", "P", "M", "O", "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

result = source.agg_by([agg.first(cols=["Y"])], by=["X"])
```

In this example, `agg.group` returns an array of values from the `Number` column (`Numbers`), and `agg.max_` returns the maximum value from the `Number` column (`MaxNumber`), as grouped by `X`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col("X", ["A", "B", "A", "C", "B", "A", "B", "B", None]),
        string_col("Y", ["M", "N", None, "N", "P", "M", None, "P", "M"]),
        int_col("Number", [55, 76, 20, 130, 230, 50, 73, 137, 214]),
    ]
)

result = source.agg_by(
    [agg.group(cols=["Numbers = Number"]), agg.max_(cols=["MaxNumber = Number"])],
    by=["X"],
)
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#aggBy(io.deephaven.api.agg.Aggregation))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.agg_by)
