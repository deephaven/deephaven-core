---
title: Create and use partitioned tables
sidebar_label: Partition
---

This guide will show you how to create and use partitioned tables. A partitioned table is a special type of Deephaven table with a column containing other tables (known as constituent tables or subtables), plus additional key column(s) that are used to index and access particular constituent tables. Essentially, a partitioned table can be visualized as a vertical stack of tables with the same schema, all housed within a single object.

Like a list of tables, partitioned tables can be [merged](#merge) together to form a new table. Unlike a list of tables, partitioned tables take advantage of [parallelization](../conceptual/query-engine/parallelization.md), which can improve query performance when leveraged properly.

> [!NOTE]
> Subtable partitioning with [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md) should not be confused with [grouping and aggregation](./dedicated-aggregations.md). The [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md) table operation partitions tables into subtables by key columns. Aggregation operators such as [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) compute summary information over groups of data within a table.

## What is a partitioned table?

A partitioned table is a special Deephaven table that holds one or more subtables, called constituent tables. Think of a partitioned table as a table with a column containing other Deephaven tables plus additional key columns that are used to index and access particular constituent tables. All constituent tables in a partitioned table _must_ have the same schema.

A partitioned table can be thought of in two ways:

1. A list of tables stacked vertically. This list can be combined into a single table using [`merge`](../reference/table-operations/partitioned-tables/merge.md).
2. A map of tables. A constituent table can be retrieved by key using [`constituentFor`](../reference/table-operations/partitioned-tables/constituentFor.md).

Partitioned tables are typically used to:

- [Parallelize queries](../conceptual/query-engine/parallelization.md) across multiple threads.
- Quickly retrieve subtables in a user interface.
- Improve the performance of filters iteratively called within loops.

## Create a partitioned table with `partitionBy`

There are two ways to create a partitioned table:

- [From a table](#from-a-table)
- [From a Kafka stream](#from-a-kafka-stream)

### From a table

The simplest way to create a partitioned table is from another table. To show this, let's first create a [new table](./new-and-empty-table.md#newtable).

```groovy test-set=1 order=source
source = newTable(
        stringCol("Letter", "A", "B", "C", "B", "C", "C", "A", "B", "A", "A"),
        intCol("Value", 5, 9, -4, -11, 3, 0, 18, -1, 1, 6),
)
```

Creating a partitioned table from the `source` table requires only one table operation: [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md). In this simple case, `source` is broken into subtables based upon the single key column `Letter`. The resulting partitioned table will have three constituent tables, one for each unique value in the `Letter` column.

```groovy test-set=1 order=keys
result = source.partitionBy("Letter")

keys = result.table.selectDistinct("Letter")
```

Partitioned tables can be constructed from more than one key column. To show this, we'll create a `source` table with more than two columns.

```groovy test-set=2 order=source
source = newTable(
        stringCol("Exchange", "Kraken", "Coinbase", "Coinbase", "Kraken", "Kraken", "Kraken", "Coinbase"),
        stringCol("Coin", "BTC", "ETH", "DOGE", "ETH", "DOGE", "BTC", "BTC"),
        doubleCol("Price", 30100.5, 1741.91, 0.068, 1739.82, 0.065, 30097.96, 30064.25),
)
```

Partitioning a table by multiple keys is done in the same manner as by one key.

```groovy test-set=2 order=keys
result = source.partitionBy("Exchange", "Coin")

keys = result.table.selectDistinct("Exchange", "Coin")
```

### From a Kafka stream

Deephaven can consume data from Kafka streams. Streaming data can be ingested into a table via [`consumeToTable`](../reference/data-import-export/Kafka/consumeToTable.md), or directly into a partitioned table via [`consumeToPartitionedTable`](../reference/data-import-export/Kafka/consumeToPartitionedTable.md).

When ingesting streaming Kafka data directly into a partitioned table, the data is partitioned by the Kafka partition number of the topic. The constituent tables are the tables per topic partition.

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

keys = pt.table.selectDistinct("Partition")
```

![The above `keys` table](../assets/how-to/partitionkeys.png)

For more information and examples on ingesting Kafka streams directly into partitioned tables, see [`consumeToPartitionedTable`](../reference/data-import-export/Kafka/consumeToPartitionedTable.md).

### Partitioned table methods

Partitioned tables have a variety of methods that differ from that of standard tables. For more information on each, without the focus on conceptual reasons for using them, see the reference docs:

- [`constituentChangesPermitted`](../reference/table-operations/partitioned-tables/constituentChangesPermitted.md)
- [`constituentColumnName`](../reference/table-operations/partitioned-tables/constituentColumnName.md)
- [`constituentDefinition`](../reference/table-operations/partitioned-tables/constituentDefinition.md)
- [`constituentFor`](../reference/table-operations/partitioned-tables/constituentFor.md)
- [`constituents`](../reference/table-operations/partitioned-tables/constituents.md)
- [`filter`](../reference/table-operations/partitioned-tables/filter.md)
- [`keyColumnNames`](../reference/table-operations/partitioned-tables/keyColumnNames.md)
- [`merge`](../reference/table-operations/partitioned-tables/merge.md)
- [`partitionedTransform`](../reference/table-operations/partitioned-tables/partitionedTransform.md)
- [`proxy`](../reference/table-operations/partitioned-tables/proxy.md)
- [`sort`](../reference/table-operations/partitioned-tables/sort.md)
- [`table`](../reference/table-operations/partitioned-tables/table.md)
- [`transform`](../reference/table-operations/partitioned-tables/transform.md)
- [`uniqueKeys`](../reference/table-operations/partitioned-tables/uniqueKeys.md)

## Examples

The following code creates two partitioned tables from other tables, which will be used in the following examples. The `quotes` and `trades` tables are constructed with [`newTable`](../reference/table-operations/create/newTable.md). They contain hypothetical stock quote and trade data. The `Ticker` column holds the key values used to partition each table into subtables.

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

Partitioned tables are primarily used to improve query performance and quickly retrieve subtables in the user interface. These reasons are conceptual in nature but are discussed in the context of tangible examples in the subsections below.

### Grab a constituent with `constituentFor`

A partitioned table contains one or more constituent tables, or subtables. When the partitioned table is created with key columns, these constituent tables each have a unique key, from which they can be obtained. The code block below grabs a constituent from each of the `ptQuotes` and `ptTrades` tables based on a key value.

```groovy test-set=1 order=quotesIbm,tradesAapl
quotesIbm = ptQuotes.constituentFor("IBM")
tradesAapl = ptTrades.constituentFor("AAPL")
```

### Merge

A partitioned table is similar to a vertically stacked list of tables with the same schema. Just as a list of tables with identical schemas can be [merged](../reference/table-operations/merge/merge.md) into a single table, so can a partitioned table. This is often the best and easiest way to get a standard table from a partitioned table.

The following example merges the `ptQuotes` and `ptTrades` tables to return tables similar to the original `quotes` and `trades` tables they were created from.

```groovy test-set=1 order=quotesNew,tradesNew
quotesNew = ptQuotes.merge()
tradesNew = ptTrades.merge()
```

### Modify a partitioned table

There are two ways to modify a partitioned table: [`transform`](../reference/table-operations/partitioned-tables/transform.md) and [`proxy`](../reference/table-operations/partitioned-tables/proxy.md). Both achieve the same results, so the choice of which to use comes down to personal preference.

#### Transform

Standard table operations can be applied to a partitioned table through a [`transform`](../reference/table-operations/partitioned-tables/transform.md), which applies a user-defined transformation function to all constituents of a partitioned table. When using [`transform`](../reference/table-operations/partitioned-tables/transform.md), any and all table operations applied in the transformation function _must_ be done from within an [execution context](../conceptual/execution-context.md).

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

The same result can be obtained via a [`PartitionedTable.Proxy`](/core/javadoc/io/deephaven/engine/table/PartitionedTable.Proxy.html) object. A partitioned table proxy is a proxy for a partitioned table that allows users to call standard table operations on it.

The following code block applies an [`update`](../reference/table-operations/select/update.md) to every constituent of the `ptQuotes` table by creating a proxy rather than applying a [`transform`](#transform).

```groovy test-set=1 order=tradesUpdated
ptTradesProxy = ptTrades.proxy()

ptTradesProxyUpdated = ptTradesProxy.update("TotalValue = Price * Size")

ptTradesUpdated = ptTradesProxyUpdated.target()

tradesUpdated = ptTradesUpdated.merge()
```

> [!NOTE]
> When using a Partitioned Table proxy, you must call `target` to obtain the underlying partitioned table.

#### Should I use transform or proxy?

A [`transform`](#transform) and a [`proxy`](#proxy) can be used to achieve the same results. So, which should you use?

- A [`transform`](../reference/table-operations/partitioned-tables/transform.md) gives greater control. Also, it gives better performance in certain cases.
- A [`proxy`](../reference/table-operations/partitioned-tables/proxy.md) provides more convenience and familiarity by allowing the use of normal table operations on a partitioned table.

If you want ease and convenience, use [`proxy`](#proxy). If you want greater control, or if performance is a high priority in your query, use [`transform`](#transform).

### Combine two partitioned tables with `partitionedTransform`

Standard tables can be combined through any of the available [joins](./joins-exact-relational.md). The same applies to partitioned tables. Where standard tables use one of the available join operations on their own, partitioned tables can be joined through a partitioned transform.

A partitioned transform is similar to a [`transform`](#transform), except the transformation function takes two tables as input and returns a single table. The transformation function can apply any table operations to either input table, so long as it returns a single table.

The following example shows the most basic usage of [`partitionedTransform`](../reference/table-operations/partitioned-tables/partitionedTransform.md): it takes two tables as input and returns only the first table that's been passed in.

```groovy test-set=1 order=quotesNew
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

defaultCtx = ExecutionContext.getContext()

partitionedTransformFunc = { t1, t2 ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        return t1
    }
}

ptQuotesNew = ptQuotes.partitionedTransform(ptTrades, partitionedTransformFunc)

quotesNew = ptQuotesNew.merge()
```

The above example is not practical. It does, however, show the basic concept that a [`partitionedTransform`](../reference/table-operations/partitioned-tables/partitionedTransform.md) takes two partitioned tables as input and returns a single partitioned table. Most usages of [`partitionedTransform`](../reference/table-operations/partitioned-tables/partitionedTransform.md) will join the two partitioned tables together, like in the example below.

```groovy test-set=1 order=result
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

defaultCtx = ExecutionContext.getContext()

partitionedTransformFunc = { t1, t2 ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        return t1.join(t2, "Ticker, Timestamp")
    }
}

ptJoined = ptTrades.partitionedTransform(ptQuotes, partitionedTransformFunc)

result = ptJoined.merge()
```

The same result can be achieved through a partitioned table proxy.

```groovy test-set=1 order=resultViaProxy
ptQuotesProxy = ptQuotes.proxy()
ptTradesProxy = ptTrades.proxy()

ptProxyJoined = ptTradesProxy.join(ptQuotesProxy, "Ticker, Timestamp")
ptJoined = ptProxyJoined.target()

resultViaProxy = ptJoined.merge()
```

## Why use partitioned tables?

So far this guide has shown how you can use partitioned tables in your queries. But it doesn't cover why you may want to use them. Initially, we discussed that partitioned tables are useful for:

- Quickly retrieving subtables in the user interface.
- Improving performance.

These will be discussed in more detail in the subsections below.

### Quickly retrieve subtables

When a partitioned table is created with key columns, it can be used like a map to retrieve constituent tables via [`constituentFor`](#grab-a-constituent-with-constituentfor). This can be useful in user interface components that need to display a subset of a table quickly.

For instance, the examples above use the `ptTrades` and `ptQuotes` tables. In the UI, the `ptTrades` subtable with the key value `IBM` is displayed. Menu options atop the table let you view the key values, merge the table, or switch between key values to see data in each constituent.

<LoopedVideo className="w-100" src='../assets/how-to/partitioned-table-viewer-g.mp4' />

### Improve performance

Partitioned tables can improve performance in a couple of different ways.

#### Parallelization

Partitioned tables can also improve query performance by parallelizing things that standard tables cannot. Take, for example, an as-of join between two tables. If the tables are partitioned by the exact match columns, then the join operation is done in parallel.

Partitioned tables are powerful, but aren't a magic wand that improves performance in all cases. Parallelization is best employed when:

- Shared resources between concurrent tasks are minimal.
- Partitioned data is dense.
- Data is sufficiently large.

We just stated that an [as-of join](./joins-timeseries-range.md#aj) can benefit from partitioning data, so let's see that in action. Consider tables of quotes and trades. The following code block creates two tables: `quotes` and `trades`. Each has 5 million rows of data, split across 4 unique exchanges and 7 unique symbols, for a total of 28 partitions. It then performs the join operation on both the standard and partitioned tables and times how long each takes.

```groovy order=null
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

Partitioned tables are faster in this case because, as mentioned earlier, the join operation is done in parallel when the tables are partitioned on the exact match columns.

If you are unsure if parallelization through partitioned tables could improve your query performance, reach out to us on [Slack](/slack) for more specific guidance.

#### Tick amplification

In grouping and ungrouping operations, the Deephaven query engine does not know which cells change. Even if only a single cell changes, an entire array is marked as modified, and large sections of the output table change. In a real-time query, this can potentially cause many unnecessary calculations to be performed. Take, for instance, the following query.

```groovy ticking-table order=null
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

t1 = timeTable("PT5s").update("A=ii%2", "X=ii")

// Group/ungroup
t2 = t1.groupBy("A").update("Y=X+1").ungroup()

// Partition/merge
t3 = t1.partitionBy("A").proxy().update("Y=X+1").target.merge()

h1 = new InstrumentedTableUpdateListenerAdapter(t1, false) {
    @Override
    public void onUpdate(TableUpdate upstream) {
        numChanges1 = len(upstream.added()) + len(upstream.modified())
        println "TICK PROPAGATION: RAW                    " + numChanges1 + " changes"
    }
}

h2 = new InstrumentedTableUpdateListenerAdapter(t2, false) {
    @Override
    public void onUpdate(TableUpdate upstream) {
        numChanges2 = len(upstream.added()) + len(upstream.modified())
        println "TICK PROPAGATION: GROUP/UNGROUP          " + numChanges2 + " changes"
    }
}

h3 = new InstrumentedTableUpdateListenerAdapter(t3, false) {
    @Override
    public void onUpdate(TableUpdate upstream) {
        numChanges3 = len(upstream.added()) + len(upstream.modified())
        println "TICK PROPAGATION: PARTITION/MERGE        " + numChanges3 + " changes"
    }
}

t1.addUpdateListener(h1)

t2.addUpdateListener(h2)

t3.addUpdateListener(h3)
```

At first, this is the output of the query:

```
TICK PROPAGATION: RAW             1 changes
TICK PROPAGATION: GROUP/UNGROUP   1 changes
TICK PROPAGATION: PARTITION/MERGE 1 changes
```

After letting the code run for a little while, this is the output:

```
TICK PROPAGATION: RAW             1 changes
TICK PROPAGATION: GROUP/UNGROUP   10 changes
TICK PROPAGATION: PARTITION/MERGE 1 changes
```

As the code runs longer, the grouping/ungrouping operation on its own continues to make more and more changes, whereas the partition/merge stays at one. Every change reported in the group/ungroup is an unnecessary calculation that performs extra work for no benefit. This is tick amplification in action. Where a group and ungroup suffers from this problem, a partition and merge does not.

## Related documentation

- [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`constituentChangesPermitted`](../reference/table-operations/partitioned-tables/constituentChangesPermitted.md)
- [`constituentColumnName`](../reference/table-operations/partitioned-tables/constituentColumnName.md)
- [`constituentDefinition`](../reference/table-operations/partitioned-tables/constituentDefinition.md)
- [`constituentFor`](../reference/table-operations/partitioned-tables/constituentFor.md)
- [`constituents`](../reference/table-operations/partitioned-tables/constituents.md)
- [`filter`](../reference/table-operations/partitioned-tables/filter.md)
- [`keyColumnNames`](../reference/table-operations/partitioned-tables/keyColumnNames.md)
- [`merge`](../reference/table-operations/partitioned-tables/merge.md)
- [`partitionedTransform`](../reference/table-operations/partitioned-tables/partitionedTransform.md)
- [`proxy`](../reference/table-operations/partitioned-tables/proxy.md)
- [`sort`](../reference/table-operations/partitioned-tables/sort.md)
- [`table`](../reference/table-operations/partitioned-tables/table.md)
- [`transform`](../reference/table-operations/partitioned-tables/transform.md)
- [`uniqueKeys`](../reference/table-operations/partitioned-tables/uniqueKeys.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html)
