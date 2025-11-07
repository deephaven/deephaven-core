---
title: partition
---

`partition` returns an aggregator that creates a partition (subtable) for an aggregation group.

## Syntax

```
partition(col: str, include_by_columns: bool = True) -> Aggregation
```

## Parameters

<ParamTable>
<Param name="col" type="str">

The column to create partitions from.

</Param>
<Param name="include_by_cols" type="boolean" optional>

Whether to include the group by columns in the result; the default is `True`.

</Param>
</ParamTable>

## Returns

An aggregator that computes a partition aggregation.

## Examples

In this example, `agg.partition` returns partitions of the `X` column, as grouped by `Letter`. The `result` table contains two rows: one for each letter. The subtables can be extracted using either [NumPy](https://numpy.org/) or [pandas](https://pandas.pydata.org/).

```python order=source,result,np_result_a,np_result_b,pd_result_a,pd_result_b
from deephaven import pandas as dhpd
from deephaven import numpy as dhnp
from deephaven import empty_table
from deephaven import agg

source = empty_table(20).update(["X = i", "Letter = (X % 2 == 0) ? `A` : `B`"])

result = source.agg_by(aggs=[agg.partition(col="X")], by=["Letter"])

np_subtables = dhnp.to_numpy(result, "X")

np_result_a = np_subtables[0][0]
np_result_b = np_subtables[1][0]

pd_subtables = dhpd.to_pandas(result, ["X"])

pd_result_a = pd_subtables["X"][0]
pd_result_b = pd_subtables["X"][1]
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to create multiple summary statistics for groups](../../../how-to-guides/combined-aggregations.md)
- [`agg_by`](./aggBy.md)
- [`partition_by`](./partitionBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggPartition(java.lang.String))
- [Pydoc](/core/pydoc/code/deephaven.agg.html#deephaven.agg.partition)
