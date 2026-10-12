---
title: Create and use partitioned tables
---

This guide shows you how to create partitioned tables, work with their constituent tables, and use them to improve query performance.

## What is a partitioned table?

A partitioned table is a special Deephaven table that holds other tables, called constituent tables or subtables. The partitioned table stores its constituent tables in one column, called the constituent column. A partitioned table usually also has key columns that index its constituent tables, so you can access a particular one by key. [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md) creates these key columns from the columns you partition on. All constituent tables in a partitioned table _must_ have the same schema.

This guide also calls each constituent table a partition.

> [!NOTE]
> Partitioning a table into subtables with [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md) is different from [grouping and aggregation](./dedicated-aggregations.md). `partitionBy` splits a table into constituent tables by key columns. Aggregation operators such as [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) compute summary information over groups of data within a table.

You can think of a partitioned table in two ways:

1. A list of tables stacked vertically. You can combine this list into a single table with [`merge`](../reference/table-operations/partitioned-tables/merge.md).
2. A map of tables. You can retrieve a constituent table by key with [`constituentFor`](../reference/table-operations/partitioned-tables/constituentFor.md).

Partitioned tables are typically used to:

- Quickly retrieve subtables, in a user interface or in place of filters called repeatedly in a loop.
- [Parallelize queries](../conceptual/query-engine/parallelization.md) across multiple threads.
- Avoid [tick amplification](../conceptual/troubleshooting-steps.md#tick-amplification) from grouping and ungrouping.

## Create a partitioned table

Common ways to create a partitioned table include:

- [From a table](#from-a-table)
- [From a Kafka stream](#from-a-kafka-stream)

You can also create one with [`partitionedAggBy`](../reference/table-operations/group-and-aggregate/partitionedAggBy.md), or from existing tables with the [`PartitionedTableFactory`](/core/javadoc/io/deephaven/engine/table/PartitionedTableFactory.html) methods [`of`](/core/javadoc/io/deephaven/engine/table/PartitionedTableFactory.html#of(io.deephaven.engine.table.Table)) and [`ofTables`](/core/javadoc/io/deephaven/engine/table/PartitionedTableFactory.html#ofTables(io.deephaven.engine.table.Table...)).

### From a table

The simplest way to create a partitioned table is from another table. To show this, let's first create a [new table](./new-and-empty-table.md#newtable).

```groovy test-set=1 order=source
source = newTable(
        stringCol("Letter", "A", "B", "C", "B", "C", "C", "A", "B", "A", "A"),
        intCol("Value", 5, 9, -4, -11, 3, 0, 18, -1, 1, 6),
)
```

Creating a partitioned table from the `source` table requires only one table operation: [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md). In this simple case, `partitionBy` splits `source` into subtables on the single key column `Letter`. The resulting partitioned table has three constituent tables, one for each unique value in the `Letter` column.

The key columns and the constituent column described in [What is a partitioned table?](#what-is-a-partitioned-table) live in an ordinary table that has one row per constituent. You access that table with the partitioned table's [`table`](../reference/table-operations/partitioned-tables/table.md) method. The example below selects the distinct key values from that table to show the partition keys.

```groovy test-set=1 order=keys
result = source.partitionBy("Letter")

keys = result.table().selectDistinct("Letter")
```

You can partition a table by more than one key column. The following `source` table has two columns to use as keys: `Exchange` and `Coin`.

```groovy test-set=2 order=source
source = newTable(
        stringCol("Exchange", "Kraken", "Coinbase", "Coinbase", "Kraken", "Kraken", "Kraken", "Coinbase"),
        stringCol("Coin", "BTC", "ETH", "DOGE", "ETH", "DOGE", "BTC", "BTC"),
        doubleCol("Price", 30100.5, 1741.91, 0.068, 1739.82, 0.065, 30097.96, 30064.25),
)
```

To partition by multiple keys, pass every key column to [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md).

```groovy test-set=2 order=keys
result = source.partitionBy("Exchange", "Coin")

keys = result.table().selectDistinct("Exchange", "Coin")
```

### From a Kafka stream

Deephaven can consume data from Kafka streams. Use [`consumeToTable`](../reference/data-import-export/Kafka/consumeToTable.md) to ingest streaming data into a table, or [`consumeToPartitionedTable`](../reference/data-import-export/Kafka/consumeToPartitionedTable.md) to ingest it directly into a partitioned table.

A partitioned table consumed from Kafka has one constituent table for each partition of the Kafka topic. A Kafka topic partition is Kafka's own unit of a topic, not a partitioned-table partition. The Kafka partition number is the key, stored in the `Partition` column.

The following example ingests data from the Kafka topic `orders` directly into a partitioned table.

```groovy skip-test docker-config=kafka
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.kafka.KafkaTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'redpanda:9092')

symbolDef = ColumnDefinition.ofString('Symbol')
sideDef = ColumnDefinition.ofString('Side')
priceDef = ColumnDefinition.ofDouble('Price')
qtyDef = ColumnDefinition.ofInt('Qty')

ColumnDefinition[] colDefs = [symbolDef, sideDef, priceDef, qtyDef]
mapping = ['jsymbol': 'Symbol', 'jside': 'Side', 'jprice': 'Price', 'jqty': 'Qty']

spec = KafkaTools.Consume.jsonSpec(colDefs, mapping, null)

pt = KafkaTools.consumeToPartitionedTable(
    kafkaProps,
    'orders',
    KafkaTools.ALL_PARTITIONS,
    KafkaTools.ALL_PARTITIONS_DONT_SEEK,
    KafkaTools.Consume.IGNORE,
    spec,
    KafkaTools.TableType.append()
)

keys = pt.table().selectDistinct("Partition")
```

![The above `keys` table](../assets/how-to/partitionkeys.png)

For more information and examples on ingesting Kafka streams directly into partitioned tables, see [`consumeToPartitionedTable`](../reference/data-import-export/Kafka/consumeToPartitionedTable.md).

## Work with partitioned tables

Partitioned tables have their own methods, which differ from those of standard tables. This section covers the most common ones. The [Related documentation](#related-documentation) section links to the reference page for each method.

The following code uses [`newTable`](../reference/table-operations/create/newTable.md) to create two tables, `quotes` and `trades`, with hypothetical stock quote and trade data. It then partitions each one on its `Ticker` column into `ptQuotes` and `ptTrades`, which the rest of this section uses.

```groovy test-set=1 order=quotes,trades
trades = newTable(
        stringCol("Ticker", "AAPL", "AAPL", "AAPL", "IBM", "IBM"),
        instantCol(
            "Timestamp",
            parseInstant("2021-04-05T09:10:00 America/New_York"),
            parseInstant("2021-04-05T09:31:00 America/New_York"),
            parseInstant("2021-04-05T16:00:00 America/New_York"),
            parseInstant("2021-04-05T16:00:00 America/New_York"),
            parseInstant("2021-04-05T16:30:00 America/New_York"),
        ),
        doubleCol("Price", 2.5, 3.7, 3.0, 100.50, 110),
        intCol("Size", 52, 14, 73, 11, 6),
)

quotes = newTable(
        stringCol("Ticker", "AAPL", "AAPL", "IBM", "IBM", "IBM"),
        instantCol(
            "Timestamp",
            parseInstant("2021-04-05T09:11:00 America/New_York"),
            parseInstant("2021-04-05T09:30:00 America/New_York"),
            parseInstant("2021-04-05T16:00:00 America/New_York"),
            parseInstant("2021-04-05T16:30:00 America/New_York"),
            parseInstant("2021-04-05T17:00:00 America/New_York"),
        ),
        doubleCol("Bid", 2.5, 3.4, 97, 102, 108),
        intCol("BidSize", 10, 20, 5, 13, 23),
        doubleCol("Ask", 2.5, 3.4, 105, 110, 111),
        intCol("AskSize", 83, 33, 47, 15, 5),
)

ptQuotes = quotes.partitionBy("Ticker")
ptTrades = trades.partitionBy("Ticker")
```

### Grab a constituent with `constituentFor`

When you create a partitioned table with [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md), each constituent has a unique key, so you can retrieve it with [`constituentFor`](../reference/table-operations/partitioned-tables/constituentFor.md). The code block below grabs a constituent from each of the `ptQuotes` and `ptTrades` tables based on a key value.

```groovy test-set=1 order=quotesIbm,tradesAapl
quotesIbm = ptQuotes.constituentFor("IBM")
tradesAapl = ptTrades.constituentFor("AAPL")
```

### Merge

Just as you can [merge](../reference/table-operations/merge/merge.md) a list of tables with identical schemas into a single table, a partitioned table's [`merge`](../reference/table-operations/partitioned-tables/merge.md) method stacks its constituents vertically into a single standard table. This is often the best and easiest way to get a standard table from a partitioned table.

The following example merges the `ptQuotes` and `ptTrades` tables to return tables similar to the original `quotes` and `trades` tables they were created from.

```groovy test-set=1 order=quotesNew,tradesNew
quotesNew = ptQuotes.merge()
tradesNew = ptTrades.merge()
```

### Modify a partitioned table

There are two ways to apply table operations to every constituent of a partitioned table: [`transform`](../reference/table-operations/partitioned-tables/transform.md) and [`proxy`](../reference/table-operations/partitioned-tables/proxy.md). A proxy supports most common table operations, and a transform can apply any function that returns a table. To choose between them, see [Should I use transform or proxy?](#should-i-use-transform-or-proxy).

#### Transform

A [`transform`](../reference/table-operations/partitioned-tables/transform.md) applies a user-defined transformation function to every constituent of a partitioned table. The function receives each constituent as a standard table, so it can call any standard table operation. When the transformation function uses operations that compile formulas or use the [query scope](./query-scope.md), such as [`update`](../reference/table-operations/select/update.md) and [`where`](../reference/table-operations/filter/where.md), it must run them inside an [execution context](../conceptual/execution-context.md). In a console session, open the context inside the function, as shown below.

The following code block uses [`transform`](../reference/table-operations/partitioned-tables/transform.md) to apply an [`update`](../reference/table-operations/select/update.md) to every constituent of the `ptTrades` table.

```groovy test-set=1 order=tradesUpdated
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

defaultCtx = ExecutionContext.getContext()

transformFunc = { t ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        return t.update("TotalValue = Price * Size")
    }
}

ptTradesUpdated = ptTrades.transform(transformFunc)

tradesUpdated = ptTradesUpdated.merge()
```

#### Proxy

A [`PartitionedTable.Proxy`](/core/javadoc/io/deephaven/engine/table/PartitionedTable.Proxy.html) is an alternative to [`transform`](#transform). You call standard table operations on the proxy, and the proxy applies each one to every constituent of the partitioned table.

The following code block applies an [`update`](../reference/table-operations/select/update.md) to every constituent of the `ptTrades` table by creating a proxy rather than applying a [`transform`](#transform).

```groovy test-set=1 order=tradesUpdated
ptTradesProxy = ptTrades.proxy()

ptTradesProxyUpdated = ptTradesProxy.update("TotalValue = Price * Size")

ptTradesUpdated = ptTradesProxyUpdated.target()

tradesUpdated = ptTradesUpdated.merge()
```

> [!NOTE]
> When using a partitioned table proxy, you must call `target` to obtain the underlying partitioned table.

#### Should I use transform or proxy?

A [`transform`](#transform) and a [`proxy`](#proxy) produce the same results for the operations a proxy supports. Choose based on what matters more for your query:

- Use [`transform`](../reference/table-operations/partitioned-tables/transform.md) for greater control and, for some queries, better performance. A transform can apply any operation, including ones a proxy doesn't provide, such as [`renameColumns`](../reference/table-operations/select/rename-columns.md).
- Use [`proxy`](../reference/table-operations/partitioned-tables/proxy.md) to call standard table operations directly on a partitioned table.

### Combine two partitioned tables with `partitionedTransform`

You combine standard tables with [joins](./joins-exact-relational.md). To join two partitioned tables constituent by constituent, use [`partitionedTransform`](../reference/table-operations/partitioned-tables/partitionedTransform.md).

A partitioned transform is similar to a [`transform`](#transform), except the transformation function takes two tables as input and returns a single table. The function can apply any table operations to either input table.

Most uses of [`partitionedTransform`](../reference/table-operations/partitioned-tables/partitionedTransform.md) join the two partitioned tables together, as in the following example.

```groovy test-set=1 order=result
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

defaultCtx = ExecutionContext.getContext()

partitionedTransformFunc = { t1, t2 ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        return t1.aj(t2, "Ticker, Timestamp")
    }
}

ptJoined = ptTrades.partitionedTransform(ptQuotes, partitionedTransformFunc)

result = ptJoined.merge()
```

You can get the same result with a partitioned table proxy.

```groovy test-set=1 order=resultViaProxy
ptQuotesProxy = ptQuotes.proxy()
ptTradesProxy = ptTrades.proxy()

ptProxyJoined = ptTradesProxy.aj(ptQuotesProxy, "Ticker, Timestamp")
ptJoined = ptProxyJoined.target()

resultViaProxy = ptJoined.merge()
```

> [!CAUTION]
> When both inputs are partitioned, the engine joins or filters each pair of matching constituents separately. This happens with [`partitionedTransform`](../reference/table-operations/partitioned-tables/partitionedTransform.md) and with a proxied operation whose right-hand table is also a proxy. If rows with the same join or filter key fall in more than one partition of either input, the result can differ from running the same operation on unpartitioned tables. This applies to joins such as [`join`](../reference/table-operations/join/join.md), [`naturalJoin`](../reference/table-operations/join/natural-join.md), and [`aj`](../reference/table-operations/join/aj.md), and to the [`whereIn`](../reference/table-operations/filter/where-in.md) and [`whereNotIn`](../reference/table-operations/filter/where-not-in.md) filters.
>
> To get correct results, partition both tables so that all rows with a given join or filter key fall in constituents with the same partition key.
>
> The [`proxy`](../reference/table-operations/partitioned-tables/proxy.md) method also has an overload that takes two `boolean` arguments:
>
> - `requireMatchingKeys` requires both partitioned tables to have the same keys when a proxied operation takes another partitioned table as input.
> - `sanityCheckJoinOperations` makes the engine check that each join, [`whereIn`](../reference/table-operations/filter/where-in.md), or [`whereNotIn`](../reference/table-operations/filter/where-not-in.md) key exists in only one partition of each input. The check applies only when the right-hand table is also a proxy.
>
> About the sanity check:
>
> - It is off by default. Calling `proxy` with no arguments is the same as calling `proxy(true, false)`.
> - Call `proxy(true, true)` to turn it on.
> - It does not verify that a key is in the same partition of the left and right tables.
> - It adds work and reduces parallelism.

## Why use partitioned tables?

So far this guide has shown how you can use partitioned tables in your queries. The following sections explain why you may want to use them, covering the uses listed in [What is a partitioned table?](#what-is-a-partitioned-table).

### Quickly retrieve subtables

If a partitioned table has key columns, you can use it like a map and retrieve constituent tables with [`constituentFor`](#grab-a-constituent-with-constituentfor). This can be useful in user interface components that need to display a subset of a table quickly.

For instance, the following animation shows the `ptTrades` partitioned table from the earlier examples opened in the UI. The UI displays the constituent for the key value `IBM`. Menu options atop the table let you view the key values, merge the table, or switch between key values to see data in each constituent.

<LoopedVideo className="w-100" src='../assets/how-to/partitioned-table-viewer-g.mp4' />

The same lookup can replace filters called repeatedly in a loop. Instead of calling [`where`](../reference/table-operations/filter/where.md) on the full table once per key, partition the table once and retrieve each key's constituent with [`constituentFor`](#grab-a-constituent-with-constituentfor).

### Parallelization

Partitioned tables can improve query performance by running an operation on each constituent in parallel. Take, for example, an [as-of join](./joins-timeseries-range.md#aj) between two tables. An as-of join matches some key columns exactly and matches the last column, usually a timestamp, to the closest value at or before it. If you partition both tables by the exactly matched columns and join them with [`partitionedTransform`](../reference/table-operations/partitioned-tables/partitionedTransform.md) or a proxy, the engine can join each pair of constituents in parallel.

The following example shows this in action. It creates two new, larger tables, `quotes` and `trades`, which replace the smaller tables of the same names used earlier on this page. Each has 5 million rows of data, split across 4 unique exchanges and 7 unique symbols, for a total of 28 partitions. It then performs the join operation on both the standard and partitioned tables and times how long each takes.

```groovy order=:log,result,quotes,trades
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

defaultCtx = ExecutionContext.getContext()

nRows = 5_000_000

quotes = emptyTable(nRows).update(
        "Timestamp = '2024-09-20T00:00:00 ET' + ii * SECOND",
        "Exchange = `Exchange_` + (i % 4)",
        "Symbol = `Sym_` + (i % 7)",
        "QuoteSize = randomInt(1, 10)",
        "QuotePrice = randomDouble(0, 100)",
)

trades = emptyTable(nRows).update(
        "Timestamp = '2024-09-20T00:00:00.1 ET' + ii * SECOND",
        "Exchange = `Exchange_` + (i % 4)",
        "Symbol = `Sym_` + (i % 7)",
        "TradeSize = randomInt(1, 10)",
        "TradePrice = randomDouble(0, 100)",
)

ptQuotes = quotes.partitionBy("Exchange", "Symbol")
ptTrades = trades.partitionBy("Exchange", "Symbol")

partitionedAj = { t1, t2 ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        return t1.aj(t2, "Exchange, Symbol, Timestamp")
    }
}

start = System.nanoTime()
result = quotes.aj(trades, "Exchange, Symbol, Timestamp")
end = System.nanoTime()

println String.format("Standard table aj: %.4f seconds.", (end - start) / 1_000_000_000.0)

start = System.nanoTime()
ptResult = ptQuotes.partitionedTransform(ptTrades, partitionedAj)
end = System.nanoTime()

println String.format("Partitioned table aj: %.4f seconds.", (end - start) / 1_000_000_000.0)
```

On a machine with multiple cores, the partitioned version is usually faster because the engine runs the join on each pair of constituents in parallel.

Partitioned tables don't improve performance in every case. Parallelization helps most when:

- Shared resources between concurrent tasks are minimal.
- Each partition holds a substantial amount of data, rather than the data being spread across many nearly empty partitions.

If you are unsure whether partitioned tables could improve your query's performance, ask on [Slack](/slack) for guidance.

### Avoid tick amplification

Partitioned tables also help avoid [tick amplification](../conceptual/troubleshooting-steps.md#tick-amplification). Tick amplification occurs when an operation produces downstream updates that touch many more cells than the upstream change. A common example is [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) followed by [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md). If you use [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md) and [`merge`](../reference/table-operations/partitioned-tables/merge.md) instead, the engine processes each constituent independently and does not check entire groups for changes.

## Related documentation

- [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`constituentChangesPermitted`](../reference/table-operations/partitioned-tables/constituentChangesPermitted.md)
- [`constituentColumnName`](../reference/table-operations/partitioned-tables/constituentColumnName.md)
- [`constituentDefinition`](../reference/table-operations/partitioned-tables/constituentDefinition.md)
- [`constituentFor`](../reference/table-operations/partitioned-tables/constituentFor.md)
- [`constituents`](../reference/table-operations/partitioned-tables/constituents.md)
- [`consumeToPartitionedTable`](../reference/data-import-export/Kafka/consumeToPartitionedTable.md)
- [`filter`](../reference/table-operations/partitioned-tables/filter.md)
- [`keyColumnNames`](../reference/table-operations/partitioned-tables/keyColumnNames.md)
- [`merge`](../reference/table-operations/partitioned-tables/merge.md)
- [`partitionedAggBy`](../reference/table-operations/group-and-aggregate/partitionedAggBy.md)
- [`partitionedTransform`](../reference/table-operations/partitioned-tables/partitionedTransform.md)
- [`proxy`](../reference/table-operations/partitioned-tables/proxy.md)
- [`sort`](../reference/table-operations/partitioned-tables/sort.md)
- [`table`](../reference/table-operations/partitioned-tables/table.md)
- [`transform`](../reference/table-operations/partitioned-tables/transform.md)
- [`uniqueKeys`](../reference/table-operations/partitioned-tables/uniqueKeys.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/PartitionedTable.html)
