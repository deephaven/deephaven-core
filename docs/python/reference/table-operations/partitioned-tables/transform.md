---
title: transform
---

The `transform` method applies a function to all constituents of a partitioned table.

## Syntax

```python syntax
transform(func: Callable[[Table], Table]) -> PartitionedTable
```

## Parameters

<ParamTable>
<Param name="func" type="Callable[[Table], Table]">

A function that takes a table as input and returns a table. The table operations applied within the function _must_ be done from within an [execution context](../../../conceptual/execution-context.md).

</Param>
</ParamTable>

## Returns

A `PartitionedTable`.

## Examples

This first example applies a transformation to add a new column to each constituent of a partitioned table.

```python order=source,result_3
from deephaven.execution_context import get_exec_ctx
from deephaven import empty_table

source = empty_table(5).update(["IntCol = i", "StrCol = `value`"])
source_partitioned = source.partition_by(["IntCol"])

ctx = get_exec_ctx()


def add_one(t):
    with ctx:
        return t.update(["IntCol2 = IntCol + 1"])


result_partitioned = source_partitioned.transform(add_one)

result_3 = result_partitioned.get_constituent([3])
```

This second example applies aggregations to each constituent of a partitioned table via `transform`.

```python order=source,result_A
from deephaven.execution_context import get_exec_ctx
from deephaven import empty_table
from deephaven import agg

import random, string


def rand_symbol() -> str:
    return random.choice(string.ascii_uppercase[:5])


source = empty_table(100).update(
    ["Sym = rand_symbol()", "X = randomInt(0, 100)", "Y = randomDouble(-50.0, 50.0)"]
)

ctx = get_exec_ctx()


def apply_aggs(t):
    with ctx:
        return t.update(["Z = X % 5"]).agg_by(
            aggs=[agg.sum_(["SumX = X"]), agg.count_("Z"), agg.avg(["AvgY = Y"])],
            by=["Sym"],
        )


partitioned_source = source.partition_by(by=["Sym"])

partitioned_result = partitioned_source.transform(func=apply_aggs)

result_A = partitioned_result.get_constituent(["A"])
```

## Related documentation

- [Execution Context](../../../conceptual/execution-context.md)
- [`empty_table`](../create/emptyTable.md)
- [`partition_by`](../group-and-aggregate/partitionBy.md)
- [`update`](../select/update.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTable.transform)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#transform(java.util.function.UnaryOperator,io.deephaven.engine.updategraph.NotificationQueue.Dependency...))
