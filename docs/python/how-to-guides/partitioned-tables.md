---
title: Create and use partitioned tables
sidebar_label: Partition
---

This guide will show you how to create and use partitioned tables. A partitioned table is a special type of Deephaven table with a column containing other tables (known as constituent tables or subtables), plus additional key column(s) that are used to index and access particular constituent tables. Essentially, a partitioned table can be visualized as a vertical stack of tables with the same schema, all housed within a single object.

Like a list of tables, partitioned tables can be [merged](#merge) together to form a new table. Unlike a list of tables, partitioned tables take advantage of [parallelization](../conceptual/query-engine/parallelization.md), which can improve query performance when leveraged properly.

> [!NOTE]
> Subtable partitioning with [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md) should not be confused with [grouping and aggregation](./dedicated-aggregations.md). The [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md) table operation partitions tables into subtables by key columns. Aggregation operators such as [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) compute summary information over groups of data within a table.

## What is a partitioned table?

A partitioned table is a special Deephaven table that holds one or more subtables, called constituent tables. Think of a partitioned table as a table with a column containing other Deephaven tables plus additional key columns that are used to index and access particular constituent tables. All constituent tables in a partitioned table _must_ have the same schema.

A partitioned table can be thought of in two ways:

1. A list of tables stacked vertically. This list can be combined into a single table using [`merge`](../reference/table-operations/partitioned-tables/partitioned-table-merge.md).
2. A map of tables. A constituent table can be retrieved by key using [`get_constituent`](../reference/table-operations/partitioned-tables/get-constituent.md).

Partitioned tables are typically used to:

- [Parallelize queries](../conceptual/query-engine/parallelization.md) across multiple threads.
- Quickly retrieve subtables in a user interface.
- Improve the performance of filters iteratively called within loops.

## Create a partitioned table with `partition_by`

There are two ways to create a partitioned table:

- [From a table](#from-a-table)
- [From a Kafka stream](#from-a-kafka-stream)

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

Creating a partitioned table from the `source` table requires only one table operation: [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md). In this simple case, `source` is broken into subtables based upon the single key column `Letter`. The resulting partitioned table will have three constituent tables, one for each unique value in the `Letter` column.

```python test-set=1 order=keys
result = source.partition_by(by=["Letter"])

keys = result.keys()
```

Partitioned tables can be constructed from more than one key column. To show this, we'll create a `source` table with more than two columns.

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

Partitioning a table by multiple keys is done in the same manner as by one key.

```python test-set=2 order=keys
result = source.partition_by(by=["Exchange", "Coin"])
keys = result.keys()
```

### From a Kafka stream

Deephaven can consume data from Kafka streams. Streaming data can be ingested into a table via [`consume`](../reference/data-import-export/Kafka/consume.md), or directly into a partitioned table via [`consume_to_partitioned_table`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.consume_to_partitioned_table).

When ingesting streaming Kafka data directly into a partitioned table, the data is partitioned by the Kafka partition number of the topic. The constituent tables are the tables per topic partition.

The following example ingests data from the Kafka topic `testTopic` directly into a partitioned table.

```python skip-test
from deephaven import kafka_consumer as kc

result_partitioned = kc.consume_to_partitioned_table(
    kafka_config={
        "bootstrap.servers": "redpanda:29092",
        "deephaven.key.column.type": "String",
        "deephaven.value.column.type": "String",
    },
    topic="testTopic",
)
print(result_partitioned.key_columns)
```

For more information and examples on ingesting Kafka streams directly into partitioned tables, see [`consume_to_partitioned_table`](/core/pydoc/code/deephaven.stream.kafka.consumer.html#deephaven.stream.kafka.consumer.consume_to_partitioned_table).

### Partitioned table methods

Partitioned tables have a variety of methods that differ from that of standard tables. For more information on each, without the focus on conceptual reasons for using them, see the reference docs:

- [`filter`](../reference/table-operations/partitioned-tables/partitioned-table-filter.md)
- [`from_constituent_tables`](../reference/table-operations/partitioned-tables/from-constituent-tables.md)
- [`from_partitioned_table`](../reference/table-operations/partitioned-tables/from-partitioned-table.md)
- [`get_constituent`](../reference/table-operations/partitioned-tables/get-constituent.md)
- [`keys`](../reference/table-operations/partitioned-tables/keys.md)
- [`partitioned_transform`](../reference/table-operations/partitioned-tables/partitioned-transform.md)
- [`merge`](../reference/table-operations/partitioned-tables/partitioned-table-merge.md)
- [`sort`](../reference/table-operations/partitioned-tables/partitioned-table-sort.md)
- [`proxy`](../reference/table-operations/partitioned-tables/proxy.md)
- [`transform`](../reference/table-operations/partitioned-tables/transform.md)
- [metadata methods](../reference/table-operations/partitioned-tables/metadata-methods.md)

## Examples

The following code creates two partitioned tables from other tables, which will be used in the following examples. The `quotes` and `trades` tables are constructed with [`new_table`](../reference/table-operations/create/newTable.md). They contain hypothetical stock quote and trade data. The `Ticker` column holds the key values used to partition each table into subtables.

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

Partitioned tables are primarily used to improve query performance and quickly retrieve subtables in the user interface. These reasons are conceptual in nature but are discussed in the context of tangible examples in the subsections below.

### Grab a constituent with `get_constituent`

A partitioned table contains one or more constituent tables, or subtables. When the partitioned table is created with key columns, these constituent tables each have a unique key, from which they can be obtained. The code block below grabs a constituent from each of the `pt_quotes` and `pt_trades` tables based on a key value.

```python test-set=1 order=quotes_ibm,trades_aapl
quotes_ibm = pt_quotes.get_constituent(key_values=["IBM"])
trades_aapl = pt_trades.get_constituent(key_values=["AAPL"])
```

### Merge

A partitioned table is similar to a vertically stacked list of tables with the same schema. Just as a list of tables with identical schemas can be [merged](../reference/table-operations/merge/merge.md) into a single table, so can a partitioned table. This is often the best and easiest way to get a standard table from a partitioned table.

The following example merges the `pt_quotes` and `pt_trades` tables to return tables similar to the original `quotes` and `trades` tables they were created from.

```python test-set=1 order=quotes_new,trades_new
quotes_new = pt_quotes.merge()
trades_new = pt_trades.merge()
```

### Modify a partitioned table

There are two ways to modify a partitioned table: [`transform`](../reference/table-operations/partitioned-tables/transform.md) and [`proxy`](../reference/table-operations/partitioned-tables/proxy.md). Both achieve the same results, so the choice of which to use comes down to personal preference.

#### Transform

Standard table operations can be applied to a partitioned table through a [`transform`](../reference/table-operations/partitioned-tables/transform.md), which applies a user-defined transformation function to all constituents of a partitioned table. When using [`transform`](../reference/table-operations/partitioned-tables/transform.md), any and all table operations applied in the transformation function _must_ be done from within an [execution context](../conceptual/execution-context.md).

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

The same result can be obtained via a [`PartitionedTableProxy`](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTableProxy) object. A partitioned table proxy is a proxy for a partitioned table that allows users to call standard table operations on it.

The following code block applies an [`update`](../reference/table-operations/select/update.md) to every constituent of the `pt_quotes` table by creating a proxy rather than applying a [`transform`](#transform).

```python test-set=1 order=trades_updated
pt_trades_proxy = pt_trades.proxy()

pt_trades_proxy_updated = pt_trades_proxy.update(["TotalValue = Price * Size"])

pt_trades_updated = pt_trades_proxy_updated.target

trades_updated = pt_trades_updated.merge()
```

> [!NOTE]
> When using a Partitioned Table proxy, you must call `.target` to obtain the underlying partitioned table.

#### Should I use transform or proxy?

A [`transform`](#transform) and a [`proxy`](#proxy) can be used to achieve the same results. So, which should you use?

- A [`transform`](../reference/table-operations/partitioned-tables/transform.md) gives greater control. Also, it gives better performance in certain cases.
- A [`proxy`](../reference/table-operations/partitioned-tables/proxy.md) provides more convenience and familiarity by allowing the use of normal table operations on a partitioned table.

If you want ease and convenience, use [`proxy`](#proxy). If you want greater control, or if performance is a high priority in your query, use [`transform`](#transform).

### Combine two partitioned tables with `partitioned_transform`

Standard tables can be combined through any of the available [joins](./joins-exact-relational.md). The same applies to partitioned tables. Where standard tables use one of the available join operations on their own, partitioned tables can be joined through a partitioned transform.

A partitioned transform is similar to a [`transform`](#transform), except the transformation function takes two tables as input and returns a single table. The transformation function can apply any table operations to either input table, so long as it returns a single table.

The following example shows the most basic usage of [`partitioned_transform`](../reference/table-operations/partitioned-tables/partitioned-transform.md): it takes two tables as input and returns only the first table that's been passed in.

```python test-set=1 order=quotes_new
from deephaven.execution_context import get_exec_ctx

ctx = get_exec_ctx()


def partitioned_transform_func(t1, t2):
    with ctx:
        return t1


pt_quotes_new = pt_quotes.partitioned_transform(
    other=pt_trades, func=partitioned_transform_func
)

quotes_new = pt_quotes_new.merge()
```

The above example is not practical. It does, however, show the basic concept that a [`partitioned_transform`](../reference/table-operations/partitioned-tables/partitioned-transform.md) takes two partitioned tables as input and returns a single partitioned table. Most usages of [`partitioned_transform`](../reference/table-operations/partitioned-tables/partitioned-transform.md) will join the two partitioned tables together, like in the example below.

```python test-set=1 order=result
from deephaven.execution_context import get_exec_ctx

ctx = get_exec_ctx()


def partitioned_transform_func(t1, t2):
    with ctx:
        return t1.join(t2, ["Ticker", "Timestamp"])


pt_joined = pt_trades.partitioned_transform(
    other=pt_quotes, func=partitioned_transform_func
)

result = pt_joined.merge()
```

The same result can be achieved through a partitioned table proxy.

```python test-set=1 order=result_via_proxy
pt_quotes_proxy = pt_quotes.proxy()
pt_trades_proxy = pt_trades.proxy()

pt_proxy_joined = pt_trades_proxy.join(pt_quotes_proxy, ["Ticker", "Timestamp"])
pt_joined = pt_proxy_joined.target

result_via_proxy = pt_joined.merge()
```

## Why use partitioned tables?

So far this guide has shown how you can use partitioned tables in your queries. But it doesn't cover why you may want to use them. Initially, we discussed that partitioned tables are useful for:

- Quickly retrieving subtables in the user interface.
- Improving performance.

These will be discussed in more detail in the subsections below.

### Quickly retrieve subtables

When a partitioned table is created with key columns, the partitioned table can be used like a map to retrieve constituent tables via [`get_constituent`](#grab-a-constituent-with-get_constituent). This can be useful in user interface components that need to quickly display a subset of a table.

For instance, the examples above use the `pt_trades` and `pt_quotes` tables. In the UI, the `pt_trades` subtable with the key value `IBM` is displayed. Menu options atop the table let you view the key values, merge the table, or switch between key values to see data in each constituent.

![A user filters by key using the partitioned table's UI](../assets/how-to/partitioned-table-viewer.gif)

### Improve performance

Partitioned tables can improve performance in a couple of different ways.

#### Parallelization

> [!CAUTION]
> Python's [Global Interpreter Lock (GIL)](https://wiki.python.org/moin/GlobalInterpreterLock) prevents threads from running concurrently. To maximize parallelization, users should be careful not to invoke Python code unnecessarily in partitioned tables.

Partitioned tables can also improve query performance by parallelizing things that standard tables cannot. Take, for example, an as-of join between two tables. If the tables are partitioned by the exact match columns, then the join operation is done in parallel.

Partitioned tables are powerful, but aren't a magic wand that improves performance in all cases. Parallelization is best employed when:

- Shared resources between concurrent tasks are minimal.
- Partitioned data is dense.
- Data is sufficiently large.

We just stated that an [as-of join](./joins-timeseries-range.md#aj) can benefit from partitioning data, so let's see that in action. Consider tables of quotes and trades. The following code block creates two tables: `quotes` and `trades`. Each has 5 million rows of data, split across 4 unique exchanges and 7 unique symbols, for a total of 28 partitions. It then performs the join operation on both the standard and partitioned tables and times how long each takes.

```python order=:log,result,quotes,trades
from deephaven.execution_context import get_exec_ctx
from deephaven import empty_table
from string import ascii_uppercase
from random import choice
from time import time

n_rows = 5_000_000

ctx = get_exec_ctx()


def rand_key(keytype, minval, maxval) -> str:
    return keytype + "_" + choice(ascii_uppercase[minval:maxval])


quotes = empty_table(n_rows).update(
    [
        "Timestamp = '2024-09-20T00:00:00 ET' + ii * SECOND",
        "Exchange = rand_key(`Exchange`, 20, 24)",
        "Symbol = rand_key(`Sym`, 0, 7)",
        "QuoteSize = randomInt(1, 10)",
        "QuotePrice = randomDouble(0, 100)",
    ]
)

trades = empty_table(n_rows).update(
    [
        "Timestamp = '2024-09-20T00:00:00.1 ET' + ii * SECOND",
        "Exchange = rand_key(`Exchange`, 20, 24)",
        "Symbol = rand_key(`Sym`, 0, 7)",
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

Partitioned tables are faster in this case because, as mentioned earlier, the join operation is done in parallel when the tables are partitioned on the exact match columns.

If you are unsure if parallelization through partitioned tables could improve your query performance, reach out to us on [Slack](/slack) for more specific guidance.

#### Tick amplification

In grouping and ungrouping operations, the Deephaven query engine does not know which cells change. Even if only a single cell changes, an entire array is marked as modified, and large sections of the output table change. In a real-time query, this can potentially cause many unnecessary calculations to be performed. Take, for instance, the following query.

```python ticking-table order=null
from deephaven import time_table
from deephaven.table_listener import listen


def print_changes(label, update, is_replay):
    added = update.added()
    modified = update.modified()
    n_added = len(added["X"]) if "X" in added else 0
    n_modified = len(modified["X"]) if "X" in modified else 0
    changes = n_added + n_modified
    print(f"TICK PROPAGATION: {label} {changes} changes")


t1 = time_table("PT5s").update(["A=ii%2", "X=ii"])

# Group/ungroup
t2 = t1.group_by("A").update("Y=X+1").ungroup()

# Partition/merge
t3 = t1.partition_by("A").proxy().update("Y=X+1").target.merge()

h1 = listen(
    t1, lambda update, is_replay: print_changes("RAW            ", update, is_replay)
)
h2 = listen(
    t2, lambda update, is_replay: print_changes("GROUP/UNGROUP  ", update, is_replay)
)
h3 = listen(
    t3, lambda update, is_replay: print_changes("PARTITION/MERGE", update, is_replay)
)
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

- [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`filter`](../reference/table-operations/partitioned-tables/partitioned-table-filter.md)
- [`from_constituent_tables`](../reference/table-operations/partitioned-tables/from-constituent-tables.md)
- [`from_partitioned_table`](../reference/table-operations/partitioned-tables/from-partitioned-table.md)
- [`get_constituent`](../reference/table-operations/partitioned-tables/get-constituent.md)
- [`keys`](../reference/table-operations/partitioned-tables/keys.md)
- [`partitioned_transform`](../reference/table-operations/partitioned-tables/partitioned-transform.md)
- [`consume_to_partitioned_table`](../reference/data-import-export/Kafka/consume-to-partitioned-table.md)
- [`merge`](../reference/table-operations/partitioned-tables/partitioned-table-merge.md)
- [`sort`](../reference/table-operations/partitioned-tables/partitioned-table-sort.md)
- [`proxy`](../reference/table-operations/partitioned-tables/proxy.md)
- [`transform`](../reference/table-operations/partitioned-tables/transform.md)
- [metadata methods](../reference/table-operations/partitioned-tables/metadata-methods.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTable)
