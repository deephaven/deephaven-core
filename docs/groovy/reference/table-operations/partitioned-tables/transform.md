---
title: transform
---

The `transform` method applies the supplied `transformer` to all constituent tables and produces a new `PartitionedTable` containing the results.

> [!NOTE]
> If no `ExecutionContext` is provided, the `transformer` will be executed in the enclosing `ExecutionContext`. In this case, the method will expect the `transformer` to produce refreshing results _if and only if_ this `PartitionedTable`'s underlying table is refreshing.

## Syntax

```
transform(transformer, dependencies...)
transform( executionContext, transformer, expectRefreshingResults, dependencies...)
```

## Parameters

<ParamTable>
<Param name="transformer" type="BinaryOperator<Table>">

The `BinaryOperator` to apply to all pairs of constituent tables.

</Param>
<Param name="dependencies" type="NotificationQueue.Dependency...">

Additional dependencies that must be satisfied before applying `transformer` to added, modified, or newly-matched constituents during update processing; use this when `transformer` uses additional `Table` or `PartitionedTable` inputs besides the constituents of `this` or `other`.

</Param>
<Param name="executionContext" type="ExecutionContext">

The `ExecutionContext` to use for the `transformer`. If provided, the `transformer` must be stateless, safe for concurrent use, and able to return a valid result for an empty input table.

</Param>
<Param name="expectRefreshingResults" type="boolean">

Whether to expect the results of applying `transformer` to be refreshing. If `true`, the resulting `PartitionedTable` will always be backed by a refreshing table. This hint is important for transforms to static inputs that might produce refreshing output, in order to ensure correct liveness management; incorrectly specifying `false` will result in exceptions.

</Param>
</ParamTable>

## Returns

A new `PartitionedTable` containing the results of applying `transformer` to all constituent tables.

## Examples

In this example, we create a table with five rows and two columns, `IntCol` and `StrCol`. We then partition the table by `IntCol` and apply a transformation that adds one to the `IntCol` values, renaming the resulting column to `IntCol2`. Finally, we retrieve the third constituent table.

```groovy order=source,result3
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

source = emptyTable(5).update('IntCol = i', 'StrCol = `value`')
sourcePartitioned = source.partitionBy('IntCol')

defaultCtx = ExecutionContext.getContext()

addOne = { t ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        return t.update('IntCol2 = IntCol + 1')
    }
}

resultPartitioned = sourcePartitioned.transform(addOne)

result3 = resultPartitioned.constituentFor(3)
```

In this example, we create a table with 100 rows and three columns, `Sym`, `X`, and `Y`. We then partition the table by `Sym` and apply aggregations to the partitioned table. Finally, we retrieve the constituent table for symbol `A`.

```groovy order=resultA,source
import static io.deephaven.api.agg.Aggregation.AggSum
import static io.deephaven.api.agg.Aggregation.AggCount
import static io.deephaven.api.agg.Aggregation.AggAvg
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

source = emptyTable(100).update('Sym = (i % 2 == 0) ? `A` : `B`', 'X = randomInt(0, 100)', 'Y = randomDouble(-50.0, 50.0)')

defaultCtx = ExecutionContext.getContext()

applyAggs = { t ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        return t.update('Z = X % 5').aggBy([AggSum('SumX = X'), AggCount('Z'), AggAvg('AvgY = Y')], 'Sym')
    }
}

partitionedSource = source.partitionBy('Sym')
partitionedResult = partitionedSource.transform(applyAggs)
resultA = partitionedResult.constituentFor('A')
```

## Related documentation

- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#transform(java.util.function.UnaryOperator,io.deephaven.engine.updategraph.NotificationQueue.Dependency...))
