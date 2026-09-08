---
title: Partitioned table transform
---

`transform` applies a unary operator to all constituents of a partitioned table.

## Syntax

```groovy syntax
transform(UnaryOperator<Table> transformer)
transform(ExecutionContext executionContext, UnaryOperator<Table> transformer, boolean expectRefreshingResults)
```

## Parameters

<ParamTable>
<Param name="transformer" type="UnaryOperator<Table>">

An operation on a table that returns a table.

</Param>
<Param name="executionContext" type="ExecutionContext">

The `ExecutionContext` to use for the transformation.

</Param>
<Param name="expectRefreshingResults" type="boolean">

Whether to expect refreshing results from the transformation. If `true`, the resulting partitioned table will always be backed by a refreshing table. This is important for transforms applied to static inputs that may produce refreshing output, because it ensures correct liveness management. Incorrectly specifying `false` will result in exceptions.

</Param>
</ParamTable>

## Returns

A `PartitionedTable`.

## Examples

This first example applies a transformation to add a new column to each constituent of a partitioned table.

```groovy order=source,newConstituent
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

source = emptyTable(5).update("IntCol = i", "StrCol = `value`")
result = source.partitionBy("IntCol")

ctx = ExecutionContext.getContext()

addOne = { t ->
    try (SafeCloseable ignored = ctx.open()) {
        return t.update("IntCol2 = IntCol + 1")
    }
}

result2 = result.transform(addOne)

newConstituent = result2.constituentFor(3)
```

This second example applies aggregations to each constituent of a partitioned table via `transform`. It specifies the execution context in the `transform` call, so no it is not needed in the closure. Additionally, refreshing results are not expected, so `false` is passed as the third argument.

```groovy order=source,result3
import static io.deephaven.api.agg.Aggregation.AggCount
import static io.deephaven.api.agg.Aggregation.AggAvg
import static io.deephaven.api.agg.Aggregation.AggSum
import io.deephaven.engine.context.ExecutionContext

import org.apache.commons.lang3.RandomStringUtils

Random random = new Random()

randSymbol = { ->
    return RandomStringUtils.random(1, 'ABCDE')
}

source = emptyTable(100).update("Sym = (String)randSymbol()", "X = randomInt(0, 100)", "Y = randomDouble(-50.0, 50.0)")

ctx = ExecutionContext.getContext()

applyAggs = { t ->
    return t.update("Z = X % 5").aggBy([AggSum("SumX = X"), AggCount("Z"), AggAvg("AvgY = Y")], "Sym")
}

partitionedSource = source.partitionBy("Sym")

partitionedResult = partitionedSource.transform(ctx, applyAggs, false)

result3 = partitionedResult.constituentFor("A")
```

- [Execution Context](../../../conceptual/execution-context.md)
- [`empty_table`](../create/emptyTable.md)
- [`partition_by`](./partitionBy.md)
- [`update`](../select/update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#transform(java.util.function.UnaryOperator,io.deephaven.engine.updategraph.NotificationQueue.Dependency...))
