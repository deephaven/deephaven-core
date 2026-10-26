---
title: Create and use partitioned tables
---

This guide shows you how to create partitioned tables, work with their constituent tables, and use them to improve query performance.

## What is a partitioned table?

A partitioned table is a special Deephaven table that holds other tables, called constituent tables or subtables. The partitioned table stores its constituent tables in one column, called the constituent column. A partitioned table usually also has key columns that index its constituent tables, so you can access a particular one by key. [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md) creates these key columns from the columns you partition on. All constituent tables in a partitioned table _must_ have the same schema.

This guide also calls each constituent table a partition.

> [!NOTE]
> Partitioning a table into subtables with [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md) is different from [grouping and aggregation](./dedicated-aggregations.md). `partition_by` splits a table into constituent tables by key columns. Aggregation operators such as [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) compute summary information over groups of data within a table.

You can think of a partitioned table in two ways:

1. A list of tables stacked vertically. You can combine this list into a single table with [`merge`](../reference/table-operations/partitioned-tables/partitioned-table-merge.md).
2. A map of tables. You can retrieve a constituent table by key with [`get_constituent`](../reference/table-operations/partitioned-tables/get-constituent.md).

Partitioned tables are typically used to:

- Quickly retrieve subtables, in a user interface or in place of filters called repeatedly in a loop.
- [Parallelize queries](../conceptual/query-engine/parallelization.md) across multiple threads.
- Avoid [tick amplification](../conceptual/troubleshooting-steps.md#tick-amplification) from grouping and ungrouping.

## Create a partitioned table

Common ways to create a partitioned table include:

- [From a table](#from-a-table)
- [From a Kafka stream](#from-a-kafka-stream)

You can also create one with [`partitioned_agg_by`](../reference/table-operations/group-and-aggregate/partitionedAggBy.md), or from existing tables with [`from_partitioned_table`](../reference/table-operations/partitioned-tables/from-partitioned-table.md) and [`from_constituent_tables`](../reference/table-operations/partitioned-tables/from-constituent-tables.md).

### From a table

The simplest way to create a partitioned table is from another table. To show this, let's first create a [new table](./new-and-empty-table.md#new_table).

```python test-set=1 order=source
from deephaven.column import int_col, string_col
from deephaven import new_table

source = new_table(
    cols=[
        string_col(
            name="Letter", data=["A", "B", "C", "B", "C", "C", "A", "B", "A", "A"]
        ),
        int_col(name="Value", data=[5, 9, -4, -11, 3, 0, 18, -1, 1, 6]),
    ]
)
```

Creating a partitioned table from the `source` table requires only one table operation: [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md). In this simple case, `partition_by` splits `source` into subtables on the single key column `Letter`. The resulting partitioned table has three constituent tables, one for each unique value in the `Letter` column.

The key columns and the constituent column described in [What is a partitioned table?](#what-is-a-partitioned-table) live in an ordinary table that has one row per constituent. You access that table through the partitioned table's [`table`](../reference/table-operations/partitioned-tables/metadata-methods.md#table) attribute. The [`keys`](../reference/table-operations/partitioned-tables/keys.md) method returns the distinct key values as a table, which the example below displays.

```python test-set=1 order=keys
result = source.partition_by(by=["Letter"])

keys = result.keys()
```

You can partition a table by more than one key column. The following `source` table has two columns to use as keys: `Exchange` and `Coin`.

```python test-set=2 order=source
from deephaven.column import double_col, string_col
from deephaven import new_table

exchanges = ["Kraken", "Coinbase", "Coinbase", "Kraken", "Kraken", "Kraken", "Coinbase"]
coins = ["BTC", "ETH", "DOGE", "ETH", "DOGE", "BTC", "BTC"]
prices = [30100.5, 1741.91, 0.068, 1739.82, 0.065, 30097.96, 30064.25]

source = new_table(
    cols=[
        string_col(name="Exchange", data=exchanges),
        string_col(name="Coin", data=coins),
        double_col(name="Price", data=prices),
    ]
)
```

To partition by multiple keys, pass every key column to [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md).

```python test-set=2 order=keys
result = source.partition_by(by=["Exchange", "Coin"])
keys = result.keys()
```

### From a Kafka stream

Deephaven can consume data from Kafka streams. Use [`consume`](../reference/data-import-export/Kafka/consume.md) to ingest streaming data into a table, or [`consume_to_partitioned_table`](../reference/data-import-export/Kafka/consume-to-partitioned-table.md) to ingest it directly into a partitioned table.

A partitioned table consumed from Kafka has one constituent table for each partition of the Kafka topic. A Kafka topic partition is Kafka's own unit of a topic, not a partitioned-table partition. The Kafka partition number is the key, stored in the `Partition` column.

The following example ingests data from the Kafka topic `orders` directly into a partitioned table.

```python skip-test
from deephaven import dtypes as dht
from deephaven import kafka_consumer as kc
from deephaven.stream.kafka.consumer import KeyValueSpec, TableType

result_partitioned = kc.consume_to_partitioned_table(
    kafka_config={
        "bootstrap.servers": "redpanda:9092",
    },
    topic="orders",
    offsets=kc.ALL_PARTITIONS_DONT_SEEK,
    key_spec=KeyValueSpec.IGNORE,
    value_spec=kc.json_spec(
        col_defs={
            "Symbol": dht.string,
            "Side": dht.string,
            "Price": dht.double,
            "Qty": dht.int32,
        },
        mapping={
            "jsymbol": "Symbol",
            "jside": "Side",
            "jprice": "Price",
            "jqty": "Qty",
        },
    ),
    table_type=TableType.append(),
)

keys = result_partitioned.keys()
```

![The above `keys` table](../assets/how-to/partitionkeys.png)

For more information and examples on ingesting Kafka streams directly into partitioned tables, see [`consume_to_partitioned_table`](../reference/data-import-export/Kafka/consume-to-partitioned-table.md).

## Work with partitioned tables

Partitioned tables have their own methods, which differ from those of standard tables. This section covers the most common ones. The [Related documentation](#related-documentation) section links to the reference page for each method.

The following code uses [`new_table`](../reference/table-operations/create/newTable.md) to create two tables, `quotes` and `trades`, with hypothetical stock quote and trade data. It then partitions each one on its `Ticker` column into `pt_quotes` and `pt_trades`, which the rest of this section uses.

```python test-set=1 order=quotes,trades
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col, datetime_col

trades = new_table(
    [
        string_col("Ticker", ["AAPL", "AAPL", "AAPL", "IBM", "IBM"]),
        datetime_col(
            "Timestamp",
            [
                "2021-04-05T09:10:00 America/New_York",
                "2021-04-05T09:31:00 America/New_York",
                "2021-04-05T16:00:00 America/New_York",
                "2021-04-05T16:00:00 America/New_York",
                "2021-04-05T16:30:00 America/New_York",
            ],
        ),
        double_col("Price", [2.5, 3.7, 3.0, 100.50, 110]),
        int_col("Size", [52, 14, 73, 11, 6]),
    ]
)

quotes = new_table(
    [
        string_col("Ticker", ["AAPL", "AAPL", "IBM", "IBM", "IBM"]),
        datetime_col(
            "Timestamp",
            [
                "2021-04-05T09:11:00 America/New_York",
                "2021-04-05T09:30:00 America/New_York",
                "2021-04-05T16:00:00 America/New_York",
                "2021-04-05T16:30:00 America/New_York",
                "2021-04-05T17:00:00 America/New_York",
            ],
        ),
        double_col("Bid", [2.5, 3.4, 97, 102, 108]),
        int_col("BidSize", [10, 20, 5, 13, 23]),
        double_col("Ask", [2.5, 3.4, 105, 110, 111]),
        int_col("AskSize", [83, 33, 47, 15, 5]),
    ]
)

pt_quotes = quotes.partition_by(by=["Ticker"])
pt_trades = trades.partition_by(by=["Ticker"])
```

### Grab a constituent with `get_constituent`

When you create a partitioned table with [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md), each constituent has a unique key, so you can retrieve it with [`get_constituent`](../reference/table-operations/partitioned-tables/get-constituent.md). The code block below grabs a constituent from each of the `pt_quotes` and `pt_trades` tables based on a key value.

```python test-set=1 order=quotes_ibm,trades_aapl
quotes_ibm = pt_quotes.get_constituent(key_values=["IBM"])
trades_aapl = pt_trades.get_constituent(key_values=["AAPL"])
```

### Merge

Just as you can [merge](../reference/table-operations/merge/merge.md) a list of tables with identical schemas into a single table, a partitioned table's [`merge`](../reference/table-operations/partitioned-tables/partitioned-table-merge.md) method stacks its constituents vertically into a single standard table. This is often the best and easiest way to get a standard table from a partitioned table.

The following example merges the `pt_quotes` and `pt_trades` tables to return tables similar to the original `quotes` and `trades` tables they were created from.

```python test-set=1 order=quotes_new,trades_new
quotes_new = pt_quotes.merge()
trades_new = pt_trades.merge()
```

### Modify a partitioned table

There are two ways to apply table operations to every constituent of a partitioned table: [`transform`](../reference/table-operations/partitioned-tables/transform.md) and [`proxy`](../reference/table-operations/partitioned-tables/proxy.md). A proxy supports most common table operations, and a transform can apply any function that returns a table. To choose between them, see [Should I use transform or proxy?](#should-i-use-transform-or-proxy).

#### Transform

A [`transform`](../reference/table-operations/partitioned-tables/transform.md) applies a user-defined transformation function to every constituent of a partitioned table. The function receives each constituent as a standard table, so it can call any standard table operation. When the transformation function uses operations that compile formulas or use the [query scope](./query-scope.md), such as [`update`](../reference/table-operations/select/update.md) and [`where`](../reference/table-operations/filter/where.md), it must run them inside an [execution context](../conceptual/execution-context.md). In a console session, open the context inside the function, as shown below.

The engine can call the transformation function on several constituents at once, and on an empty constituent. Write it so that it keeps no state between calls, is safe to run concurrently, and returns a valid table for an empty input.

The following code block uses [`transform`](../reference/table-operations/partitioned-tables/transform.md) to apply an [`update`](../reference/table-operations/select/update.md) to every constituent of the `pt_trades` table.

```python test-set=1 order=trades_updated
from deephaven.execution_context import get_exec_ctx

ctx = get_exec_ctx()


def transform_func(t):
    with ctx:
        return t.update(["TotalValue = Price * Size"])


pt_trades_updated = pt_trades.transform(func=transform_func)

trades_updated = pt_trades_updated.merge()
```

#### Proxy

A [`PartitionedTableProxy`](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTableProxy) is an alternative to [`transform`](#transform). You call standard table operations on the proxy, and the proxy applies each one to every constituent of the partitioned table.

The following code block applies an [`update`](../reference/table-operations/select/update.md) to every constituent of the `pt_trades` table by creating a proxy rather than applying a [`transform`](#transform).

```python test-set=1 order=trades_updated
pt_trades_proxy = pt_trades.proxy()

pt_trades_proxy_updated = pt_trades_proxy.update(["TotalValue = Price * Size"])

pt_trades_updated = pt_trades_proxy_updated.target

trades_updated = pt_trades_updated.merge()
```

> [!NOTE]
> When using a partitioned table proxy, you must access the `target` attribute to obtain the underlying partitioned table.

#### Should I use transform or proxy?

A [`transform`](#transform) and a [`proxy`](#proxy) produce the same results for the operations a proxy supports. Choose based on what matters more for your query:

- Use [`transform`](../reference/table-operations/partitioned-tables/transform.md) for greater control and, for some queries, better performance. A transform can apply any operation, including ones a proxy doesn't provide, such as [`rename_columns`](../reference/table-operations/select/rename-columns.md) and [`drop_columns`](../reference/table-operations/select/drop-columns.md).
- Use [`proxy`](../reference/table-operations/partitioned-tables/proxy.md) to call standard table operations directly on a partitioned table.

### Combine two partitioned tables with `partitioned_transform`

You combine standard tables with [joins](./joins-exact-relational.md). To join two partitioned tables constituent by constituent, use [`partitioned_transform`](../reference/table-operations/partitioned-tables/partitioned-transform.md).

A partitioned transform is similar to a [`transform`](#transform), except the transformation function takes two tables as input and returns a single table. The function can apply any table operations to either input table. It has the same requirements as a transform function: it keeps no state between calls, is safe to run concurrently, and returns a valid table for empty inputs.

Most uses of [`partitioned_transform`](../reference/table-operations/partitioned-tables/partitioned-transform.md) join the two partitioned tables together, as in the following example.

```python test-set=1 order=result
from deephaven.execution_context import get_exec_ctx

ctx = get_exec_ctx()


def partitioned_transform_func(t1, t2):
    with ctx:
        return t1.aj(t2, ["Ticker", "Timestamp"])


pt_joined = pt_trades.partitioned_transform(
    other=pt_quotes, func=partitioned_transform_func
)

result = pt_joined.merge()
```

You can get the same result with a partitioned table proxy.

```python test-set=1 order=result_via_proxy
pt_quotes_proxy = pt_quotes.proxy()
pt_trades_proxy = pt_trades.proxy()

pt_proxy_joined = pt_trades_proxy.aj(pt_quotes_proxy, ["Ticker", "Timestamp"])
pt_joined = pt_proxy_joined.target

result_via_proxy = pt_joined.merge()
```

> [!CAUTION]
> When both inputs are partitioned, the engine joins or filters each pair of matching constituents separately. This happens with [`partitioned_transform`](../reference/table-operations/partitioned-tables/partitioned-transform.md) and with a proxied operation whose right-hand table is also a proxy. If rows with the same join or filter key fall in more than one partition of either input, the result can differ from running the same operation on unpartitioned tables. This applies to joins such as [`join`](../reference/table-operations/join/join.md), [`natural_join`](../reference/table-operations/join/natural-join.md), and [`aj`](../reference/table-operations/join/aj.md), and to the [`where_in`](../reference/table-operations/filter/where-in.md) and [`where_not_in`](../reference/table-operations/filter/where-not-in.md) filters.
>
> To get correct results, partition both tables so that all rows with a given join or filter key fall in constituents with the same partition key.
>
> The `sanity_check_joins` argument to [`proxy`](../reference/table-operations/partitioned-tables/proxy.md) controls an extra check on proxied joins and filters:
>
> - It defaults to `True`.
> - When it is `True` and the right-hand table is also a proxy, the engine checks that each join, [`where_in`](../reference/table-operations/filter/where-in.md), or [`where_not_in`](../reference/table-operations/filter/where-not-in.md) key exists in only one partition of each input.
> - The check does not verify that a key is in the same partition of the left and right tables.
> - The check adds work and reduces parallelism. Set `sanity_check_joins=False` to skip it.

## Why use partitioned tables?

So far this guide has shown how you can use partitioned tables in your queries. The following sections explain why you may want to use them, covering the uses listed in [What is a partitioned table?](#what-is-a-partitioned-table).

### Quickly retrieve subtables

If a partitioned table has key columns, you can use it like a map and retrieve constituent tables with [`get_constituent`](#grab-a-constituent-with-get_constituent). This can be useful in user interface components that need to quickly display a subset of a table.

For instance, the following animation shows the `pt_trades` partitioned table from the earlier examples opened in the UI. The UI displays the constituent for the key value `IBM`. Menu options atop the table let you view the key values, merge the table, or switch between key values to see data in each constituent.

![A user filters by key using the partitioned table's UI](../assets/how-to/partitioned-table-viewer.gif)

The same lookup can replace filters called repeatedly in a loop. Instead of calling [`where`](../reference/table-operations/filter/where.md) on the full table once per key, partition the table once and retrieve each key's constituent with [`get_constituent`](#grab-a-constituent-with-get_constituent).

### Parallelization

Partitioned tables can improve query performance by running an operation on each constituent in parallel. Take, for example, an [as-of join](./joins-timeseries-range.md#aj) between two tables. An as-of join matches some key columns exactly and matches the last column, usually a timestamp, to the closest value at or before it. If you partition both tables by the exactly matched columns and join them with [`partitioned_transform`](../reference/table-operations/partitioned-tables/partitioned-transform.md) or a proxy, the engine can join each pair of constituents in parallel.

The following example shows this in action. It creates two new, larger tables, `quotes` and `trades`, which replace the smaller tables of the same names used earlier on this page. Each has 5 million rows of data, split across 4 unique exchanges and 7 unique symbols, for a total of 28 partitions. It then performs the join operation on both the standard and partitioned tables and times how long each takes.

```python order=:log,result,quotes,trades
from deephaven.execution_context import get_exec_ctx
from deephaven import empty_table
from time import time

n_rows = 5_000_000

ctx = get_exec_ctx()


quotes = empty_table(n_rows).update(
    [
        "Timestamp = '2024-09-20T00:00:00 ET' + ii * SECOND",
        "Exchange = `Exchange_` + (i % 4)",
        "Symbol = `Sym_` + (i % 7)",
        "QuoteSize = randomInt(1, 10)",
        "QuotePrice = randomDouble(0, 100)",
    ]
)

trades = empty_table(n_rows).update(
    [
        "Timestamp = '2024-09-20T00:00:00.1 ET' + ii * SECOND",
        "Exchange = `Exchange_` + (i % 4)",
        "Symbol = `Sym_` + (i % 7)",
        "TradeSize = randomInt(1, 10)",
        "TradePrice = randomDouble(0, 100)",
    ]
)

pt_quotes = quotes.partition_by(["Exchange", "Symbol"])
pt_trades = trades.partition_by(["Exchange", "Symbol"])


def partitioned_aj(t1, t2):
    with ctx:
        return t1.aj(t2, ["Exchange", "Symbol", "Timestamp"])


start = time()
result = quotes.aj(trades, ["Exchange", "Symbol", "Timestamp"])
end = time()

print(f"Standard table aj: {(end - start):.4f} seconds.")

start = time()
pt_result = pt_quotes.partitioned_transform(pt_trades, partitioned_aj)
end = time()

print(f"Partitioned table aj: {(end - start):.4f} seconds.")
```

On a machine with multiple cores, the partitioned version is usually faster because the engine runs the join on each pair of constituents in parallel.

Partitioned tables don't improve performance in every case. Parallelization helps most when:

- Shared resources between concurrent tasks are minimal.
- Each partition holds a substantial amount of data, rather than the data being spread across many nearly empty partitions.

> [!CAUTION]
> Python's [Global Interpreter Lock (GIL)](https://docs.python.org/3/glossary.html#term-global-interpreter-lock) prevents threads from running Python code concurrently, unless the Python build is free-threaded. To maximize parallelization, keep transformation functions limited to table operations, as `partitioned_aj` does. Don't call Python functions from the formulas those operations evaluate. For example, avoid a formula such as `update(["Y = my_func(X)"])`.

If you are unsure whether partitioned tables could improve your query's performance, ask on [Slack](/slack) for guidance.

### Avoid tick amplification

Partitioned tables also help avoid [tick amplification](../conceptual/troubleshooting-steps.md#tick-amplification). Tick amplification occurs when an operation produces downstream updates that touch many more cells than the upstream change. A common example is [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) followed by [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md). If you use [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md) and [`merge`](../reference/table-operations/partitioned-tables/partitioned-table-merge.md) instead, the engine processes each constituent independently and does not check entire groups for changes.

## Related documentation

- [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`consume_to_partitioned_table`](../reference/data-import-export/Kafka/consume-to-partitioned-table.md)
- [`filter`](../reference/table-operations/partitioned-tables/partitioned-table-filter.md)
- [`from_constituent_tables`](../reference/table-operations/partitioned-tables/from-constituent-tables.md)
- [`from_partitioned_table`](../reference/table-operations/partitioned-tables/from-partitioned-table.md)
- [`get_constituent`](../reference/table-operations/partitioned-tables/get-constituent.md)
- [`keys`](../reference/table-operations/partitioned-tables/keys.md)
- [`merge`](../reference/table-operations/partitioned-tables/partitioned-table-merge.md)
- [metadata methods](../reference/table-operations/partitioned-tables/metadata-methods.md)
- [`partitioned_agg_by`](../reference/table-operations/group-and-aggregate/partitionedAggBy.md)
- [`partitioned_transform`](../reference/table-operations/partitioned-tables/partitioned-transform.md)
- [`proxy`](../reference/table-operations/partitioned-tables/proxy.md)
- [`sort`](../reference/table-operations/partitioned-tables/partitioned-table-sort.md)
- [`transform`](../reference/table-operations/partitioned-tables/transform.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTable)
