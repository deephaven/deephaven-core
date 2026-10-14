---
title: transform
---

The `transform` method applies the supplied `transformer` to all constituent tables of a [`PartitionedTable`](../../../how-to-guides/partitioned-tables.md) and produces a new `PartitionedTable` containing the results.

The first overload records the enclosing [`ExecutionContext`](../../../conceptual/execution-context.md) and opens it each time `transformer` runs, unless that context is systemic. The default context of a script session is systemic, so when this overload is called from the console, `transformer` must open an `ExecutionContext` itself before it uses the query language, as the first two examples below show. This overload expects `transformer` to produce refreshing results if and only if the underlying table of the `PartitionedTable` is refreshing.

The second overload opens the supplied `ExecutionContext` each time `transformer` runs and takes an explicit `expectRefreshingResults` flag.

## Syntax

```groovy syntax
transform(UnaryOperator<Table> transformer, NotificationQueue.Dependency... dependencies)
transform(ExecutionContext executionContext, UnaryOperator<Table> transformer, boolean expectRefreshingResults, NotificationQueue.Dependency... dependencies)
```

## Parameters

<ParamTable>
<Param name="executionContext" type="ExecutionContext">

The `ExecutionContext` in which to run `transformer`. If `NULL`, `transform` does not open an `ExecutionContext` for `transformer`.

</Param>
<Param name="transformer" type="UnaryOperator<Table>">

The operation to apply to each constituent table. It takes a table and returns a table. `transformer` must be stateless, safe for concurrent use, and able to return a valid result for an empty input table.

</Param>
<Param name="expectRefreshingResults" type="boolean">

Whether to expect that the results of applying `transformer` may be refreshing. If `true`, the resulting `PartitionedTable` is always backed by a refreshing table. This hint is important for transforms of static inputs that might produce refreshing output, because it ensures correct liveness management. Incorrectly specifying `false` results in exceptions.

</Param>
<Param name="dependencies" type="NotificationQueue.Dependency...">

Additional dependencies that must be satisfied before applying `transformer` to added or modified constituents during update processing. Use this when `transformer` uses additional `Table` or `PartitionedTable` inputs besides the constituents of this `PartitionedTable`.

</Param>
</ParamTable>

## Returns

A new `PartitionedTable` containing the results of applying `transformer` to all constituent tables.

## Examples

The following example partitions a table by `IntCol` and applies a transformation that adds a new column, `IntCol2`, to each constituent. It then retrieves the constituent for key `3`. The closure opens the script session's `ExecutionContext` before it calls [`update`](../select/update.md).

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

The following example applies the same aggregations as the previous example with the second overload. It passes the script session's `ExecutionContext` to `transform`, so the closure does not open a context itself. Because the source table is static and the aggregations do not produce refreshing results, it passes `false` for `expectRefreshingResults`.

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

- [Create and use partitioned tables](../../../how-to-guides/partitioned-tables.md)
- [Execution Context](../../../conceptual/execution-context.md)
- [`emptyTable`](../create/emptyTable.md)
- [`partitionBy`](../group-and-aggregate/partitionBy.md)
- [`constituentFor`](./constituentFor.md)
- [`partitionedTransform`](./partitionedTransform.md)
- [`update`](../select/update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#transform(java.util.function.UnaryOperator,io.deephaven.engine.updategraph.NotificationQueue.Dependency...))
