---
title: transform
---

The `transform` method applies the supplied `transformer` to all constituent tables of a `PartitionedTable` and produces a new `PartitionedTable` containing the results.

The first overload uses the enclosing `ExecutionContext` and expects `transformer` to produce refreshing results if and only if the underlying table of the `PartitionedTable` is refreshing. The second overload invokes `transformer` in the `ExecutionContext` you provide and lets you state whether to expect refreshing results.

In both cases, `transformer` must be stateless, safe for concurrent use, and able to return a valid result for an empty input table.

## Syntax

```groovy syntax
transform(UnaryOperator<Table> transformer, Dependency... dependencies)
transform(ExecutionContext executionContext, UnaryOperator<Table> transformer, boolean expectRefreshingResults, Dependency... dependencies)
```

## Parameters

<ParamTable>
<Param name="transformer" type="UnaryOperator<Table>">

The operation to apply to each constituent table. It takes a table and returns a table.

</Param>
<Param name="dependencies" type="NotificationQueue.Dependency...">

Additional dependencies that must be satisfied before applying `transformer` to added or modified constituents during update processing. Use this when `transformer` uses additional `Table` or `PartitionedTable` inputs besides the constituents of this `PartitionedTable`.

</Param>
<Param name="executionContext" type="ExecutionContext">

The `ExecutionContext` in which to invoke `transformer`.

</Param>
<Param name="expectRefreshingResults" type="boolean">

Whether to expect that the results of applying `transformer` may be refreshing. If `true`, the resulting `PartitionedTable` is always backed by a refreshing table. This hint is important for transforms of static inputs that might produce refreshing output, because it ensures correct liveness management. Incorrectly specifying `false` results in exceptions.

</Param>
</ParamTable>

## Returns

A new `PartitionedTable` containing the results of applying `transformer` to all constituent tables.

## Examples

The following example partitions a table by `IntCol` and applies a transformation that adds a new column, `IntCol2`, to each constituent. It then retrieves the constituent for key `3`. The transformer opens the enclosing `ExecutionContext` so it can use the query language inside the closure.

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

The following example partitions a table by `Sym` and applies aggregations to each constituent. It then retrieves the constituent for symbol `A`.

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

The following example uses the second overload. It passes the `ExecutionContext` to `transform` directly, so the transformer doesn't need to open it. The source table is static and the aggregations don't produce refreshing results, so it passes `false` for `expectRefreshingResults`.

```groovy order=resultB,source
import static io.deephaven.api.agg.Aggregation.AggSum
import static io.deephaven.api.agg.Aggregation.AggCount
import static io.deephaven.api.agg.Aggregation.AggAvg
import io.deephaven.engine.context.ExecutionContext

source = emptyTable(100).update('Sym = (i % 2 == 0) ? `A` : `B`', 'X = i', 'Y = i * 0.5')

defaultCtx = ExecutionContext.getContext()

applyAggs = { t ->
    return t.update('Z = X % 5').aggBy([AggSum('SumX = X'), AggCount('Z'), AggAvg('AvgY = Y')], 'Sym')
}

partitionedSource = source.partitionBy('Sym')
partitionedResult = partitionedSource.transform(defaultCtx, applyAggs, false)
resultB = partitionedResult.constituentFor('B')
```

## Related documentation

- [Execution Context](../../../conceptual/execution-context.md)
- [`emptyTable`](../create/emptyTable.md)
- [`partitionBy`](../group-and-aggregate/partitionBy.md)
- [`constituentFor`](./constituentFor.md)
- [`partitionedTransform`](./partitionedTransform.md)
- [`update`](../select/update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#transform(java.util.function.UnaryOperator,io.deephaven.engine.updategraph.NotificationQueue.Dependency...))
