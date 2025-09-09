---
title: partitionedTransform
---

The `partitionedTransform` method applies the supplied `transformer` to all constituent tables found in `this` (the `PartitionedTable` instance that calls `partitionedTransform`) and `other` (the other partitioned table that will be merged/joined with `this`) with the same key column values. It produces a new `PartitionedTable` containing the results.

`other`'s key columns must match `this` `PartitionedTable`'s key columns. Two matching mechanisms are supported, and will be attempted in the order listed:

- Match by column name. Both `PartitionedTable`s must have all the same key column names. Like-named columns must have the same data type and component type.
- Match by column order. Both `PartitionedTable`s must have their matchable columns in the same order within their key column names. Like-positioned columns must have the same data type and component type.
- This overload uses the enclosing `ExecutionContext` and expects the transformer to produce refreshing results if and only if `this` or `other` has a refreshing underlying table.

## Syntax

```
partitionedTransform(other, transformer, dependencies...)
partitionedTransform(other, executionContext, transformer, expectRefreshingResults, dependencies...)
```

## Parameters

<ParamTable>
<Param name="other" type="PartitionedTable">

The other `PartitionedTable` to find constituents in.

</Param>
<Param name="transformer" type="BinaryOperator<Table>">

The `BinaryOperator` to apply to all pairs of constituent tables.

</Param>
<Param name="dependencies" type="NotificationQueue.Dependency...">

Additional dependencies that must be satisfied before applying `transformer` to added, modified, or newly-matched constituents during update processing; use this when `transformer` uses additional `Table` or `PartitionedTable` inputs besides the constituents of `this` or `other`.

</Param>
<Param name="executionContext" type="ExecutionContext">

The `ExecutionContext` to use for the `transformer`.

</Param>
<Param name="expectRefreshingResults" type="boolean">

Whether to expect the results of applying `transformer` to be refreshing. If `true`, the resulting `PartitionedTable` will always be backed by a refreshing table. This hint is important for transforms to static inputs that might produce refreshing output, in order to ensure correct liveness management; incorrectly specifying `false` will result in exceptions.

</Param>
</ParamTable>

## Returns

A new `PartitionedTable` containing the results of applying `transformer` to all pairs of constituent tables found in `this` and `other` with the same key column values.

## Examples

In the following example, two tables are created and partitioned by the `X` column. The `partitionedTransform` method is used to apply a transformer that returns the first table in the partitioned table. The result is a new partitioned table that is coalesced into a single table with [`merge`](./merge.md).

```groovy order=result,t1,t2
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

t1 = emptyTable(10).update('X = i % 2', 'Y = randomDouble(0.0, 100.0)')
t2 = emptyTable(10).update('X = i % 2', 'Z = randomDouble(100.0, 500.0)')

pt1 = t1.partitionBy('X')
pt2 = t2.partitionBy('X')

defaultCtx = ExecutionContext.getContext()

transformer = { t1, t2 ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        return t1
    }
}

pt3 = pt1.partitionedTransform(pt2, transformer)

result = pt3.merge()
```

In this example, the transformer returns the result of joining the two tables on the `"X"` column. The result is a new partitioned table that is then coalesced into a single table with [`merge`](./merge.md).

```groovy order=result,t1,t2
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

t1 = emptyTable(10).update('X = i % 2', 'Y = randomDouble(0.0, 100.0)')
t2 = emptyTable(10).update('X = i % 2', 'Z = randomDouble(100.0, 500.0)')

pt1 = t1.partitionBy('X')
pt2 = t2.partitionBy('X')

defaultCtx = ExecutionContext.getContext()

transformer = { t1, t2 ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        return t1.join(t2, 'X')
    }
}

pt3 = pt1.partitionedTransform(pt2, transformer)

result = pt3.merge()
```

In this example, the transformer returns the result of an [`aj`](../join/aj.md) between two tables. The result is a new partitioned table that is then coalesced into a single table with [`merge`](./merge.md).

```groovy order=result,trades,quotes
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

trades = newTable(
    stringCol('Ticker', 'AAPL', 'AAPL', 'AAPL', 'IBM', 'IBM'),
    instantCol('Timestamp',
        parseInstant('2021-04-05T09:10:00 ET'), parseInstant('2021-04-05T09:31:00 ET'),
        parseInstant('2021-04-05T16:00:00 ET'), parseInstant('2021-04-05T16:00:00 ET'),
        parseInstant('2021-04-05T16:30:00 ET')),
    doubleCol('Price', 2.5, 3.7, 3.0, 100.50, 110),
    intCol('Size', 52, 14, 73, 11, 6)
)

quotes = newTable(
    stringCol('Ticker', 'AAPL', 'AAPL', 'IBM', 'IBM', 'IBM'),
    instantCol('Timestamp',
        parseInstant('2021-04-05T09:11:00 ET'), parseInstant('2021-04-05T09:30:00 ET'),
        parseInstant('2021-04-05T16:00:00 ET'), parseInstant('2021-04-05T16:30:00 ET'),
        parseInstant('2021-04-05T17:00:00 ET')),
    doubleCol('Bid', 2.5, 3.4, 97, 102, 108),
    intCol('BidSize', 10, 20, 5, 13, 23),
    doubleCol('Ask', 2.5, 3.4, 105, 110, 111),
    intCol('AskSize', 83, 33, 47, 15, 5)
)

ptTrades = trades.partitionBy('Ticker')
ptQuotes = quotes.partitionBy('Ticker')

defaultCtx = ExecutionContext.getContext()

ptAsOfJoin = { quotes, trades ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        return trades.aj(quotes, 'Ticker, Timestamp')
    }
}

pt3 = ptTrades.partitionedTransform(ptQuotes, ptAsOfJoin)

result = pt3.merge()
```

## Related documentation

- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#partitionedTransform(io.deephaven.engine.table.PartitionedTable,java.util.function.BinaryOperator,io.deephaven.engine.updategraph.NotificationQueue.Dependency...))
