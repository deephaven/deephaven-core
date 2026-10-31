---
id: synchronizing-tables
title: Synchronize multiple tables
---

In a real-time application, related tables often [tick](../conceptual/table-update-model.md) at different rates. This guide shows how to use [`SyncTableFilter`](/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.html) and [`LeaderTableFilter`](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.html) to coordinate updates across those tables, so your queries see only rows that belong together.

> [!NOTE]
> These filters have no Python API wrapper, so you use them through [`jpy`](./use-jpy.md):
>
> - Pass each input's underlying Java object to the builder: `table.j_table` for a table, or `partitioned_table.j_partitioned_table` for a partitioned table.
> - Wrap each result to use it in Python: [`Table`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table) for a table, or [`PartitionedTable`](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTable) for a partitioned table.

## The synchronization problem

Deephaven does not provide cross-table or cross-partition transactions. A partition is one independently updating slice of a table's data, such as one Kafka partition or the data for one date.

To maximize throughput, Deephaven typically updates each source table independently. It updates each partition of a source independently too. As a result, a row created later in one partition may appear in your query before a row created earlier in a different partition.

This independence can cause consistency issues when you have multiple tables that contain correlated data. For example:

- A trading system might have separate tables for orders, executions, and messages that all share a common transaction ID.
- A data pipeline might split events across multiple tables, each tagged with a sequence number.
- A multi-source system might need to wait until all sources report data for a given timestamp.

Both [`SyncTableFilter`](/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.html) and [`LeaderTableFilter`](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.html) solve this problem by ensuring that only coordinated rows appear in the filtered results.

Both filters match rows on two kinds of columns. A key column groups related rows, such as a `Symbol` column or a client session ID column. An ID column holds a number that never decreases within each key, such as a sequence number or transaction ID. Rows from different tables that have the same key and the same ID belong together.

## Choose a filter

Choose the filter based on your table relationships:

- **Use [`SyncTableFilter`](/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.html)** when all tables are peers. Each table contributes equally to determining which rows to show.

- **Use [`LeaderTableFilter`](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.html)** when one table should control synchronization. The leader table contains ID values that dictate which rows from follower tables to show. This is useful when one table acts as a coordination log or contains the authoritative sequence of events.

## Requirements

Both [`SyncTableFilter`](/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.html) and [`LeaderTableFilter`](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.html) require input tables that meet these conditions:

- **[Add-only](../conceptual/table-types.md#specialization-2-add-only) tables**: Tables must not modify, shift, or remove rows.
- **Long ID columns**: Every ID column must be of type `long`, including the leader table's ID columns.
- **Non-decreasing IDs**: For each key, ID values must never go backward. Many rows can share one ID, but once the filter matches a later ID, it ignores rows that arrive with an earlier ID.
- **Atomic updates**: All rows for a given ID of a given table must appear in the same update. If some of those rows arrive in a later update, the results show a partial set of rows for that ID until the late rows arrive.
- **Matching key columns**: Each table supplies the same number of key columns, in the same order, with the same types. Column names can differ between tables.

### Make your input tables add-only

A table meets the add-only requirement when it is static or has the add-only or append-only attribute. [`LeaderTableFilter`](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.html) checks this same condition. Qualifying tables include:

- A static table.
- A refreshing table marked [add-only](../conceptual/table-types.md#specialization-2-add-only) or [append-only](../conceptual/table-types.md#specialization-1-append-only).
- A [time table](./time-table.md) that is not a [blink table](../conceptual/table-types.md#specialization-3-blink). [`time_table`](../reference/table-operations/create/timeTable.md) marks it append-only.
- The result of filtering a qualifying table with [`where`](../reference/table-operations/filter/where.md), as long as the [`where`](../reference/table-operations/filter/where.md) filters are not themselves refreshing.
- The result of [`update`](../reference/table-operations/select/update.md), [`update_view`](../reference/table-operations/select/update-view.md), [`view`](../reference/table-operations/select/view.md), [`select`](../reference/table-operations/select/select.md), [`drop_columns`](../reference/table-operations/select/drop-columns.md), or [`rename_columns`](../reference/table-operations/select/rename-columns.md) on a qualifying table. A formula that reads another row, such as `Price_[ii+1]`, can drop the attribute.

For either filter, mark a refreshing table that you know only adds rows by calling [`assertAddOnly`](/core/javadoc/io/deephaven/engine/table/Table.html#assertAddOnly()) through `j_table`. If the table later breaks that promise, the marked table fails instead of passing wrong rows to the filter:

```python syntax
from deephaven.table import Table

add_only_source = Table(source.j_table.assertAddOnly())
```

The two filters enforce the add-only requirement differently:

- [`LeaderTableFilter`](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.html) checks it when you call [`build`](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.TableBuilder.html#build()), and throws an `IllegalArgumentException` if the leader or a follower table fails it.
- [`SyncTableFilter`](/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.html) doesn't check it when you call [`build`](/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.Builder.html#build()). A table that breaks the requirement causes one of these problems:
  - If an input table removes or shifts rows, the synchronized result tables fail with an `IllegalStateException` and stop updating.
  - If an input table modifies rows, the filter processes them again as if they were new rows, which can leave stale rows in the results.

## `SyncTableFilter`

[`SyncTableFilter`](/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.html) synchronizes peer tables whose rows carry matching ID values for each key.

### How it works

For each key, the filter finds the highest ID for which every input table has a matching row, and passes through only the rows at that ID. When the tables receive new data and reach a higher common ID, the filter removes the previous ID's rows and adds the new ID's rows.

### Example

This example synchronizes three tables that share `Symbol` as a key and use `SeqNum` as the ID:

```python order=synced_prices,synced_volumes,synced_bid_ask
import jpy
from deephaven import new_table
from deephaven.column import string_col, long_col, double_col
from deephaven.table import Table

SyncTableFilterBuilder = jpy.get_type(
    "io.deephaven.engine.table.impl.util.SyncTableFilter$Builder"
)

price_data = new_table(
    [
        string_col("Symbol", ["AAPL", "AAPL", "AAPL", "GOOGL", "GOOGL"]),
        long_col("SeqNum", [1, 2, 3, 1, 2]),
        double_col("Price", [150.0, 151.0, 152.0, 2800.0, 2805.0]),
    ]
)

volume_data = new_table(
    [
        string_col("Symbol", ["AAPL", "AAPL", "GOOGL", "GOOGL"]),
        long_col("SeqNum", [1, 2, 1, 2]),
        long_col("Volume", [1000000, 1100000, 500000, 520000]),
    ]
)

bid_ask_data = new_table(
    [
        string_col("Symbol", ["AAPL", "AAPL", "GOOGL"]),
        long_col("SeqNum", [1, 2, 1]),
        double_col("Bid", [149.95, 150.95, 2799.50]),
        double_col("Ask", [150.05, 151.05, 2800.50]),
    ]
)

builder = SyncTableFilterBuilder("SeqNum", "Symbol")
builder.addTable("prices", price_data.j_table)
builder.addTable("volumes", volume_data.j_table)
builder.addTable("bidAsk", bid_ask_data.j_table)

result = builder.build()

synced_prices = Table(result.get("prices"))
synced_volumes = Table(result.get("volumes"))
synced_bid_ask = Table(result.get("bidAsk"))
```

In this example:

- For `AAPL`, `price_data` has `SeqNum` 1, 2, and 3, but `volume_data` and `bid_ask_data` only go up to `SeqNum` 2. The highest ID common to all three is 2, so only the `SeqNum` 2 rows appear in the synchronized results.
- For `GOOGL`, `price_data` and `volume_data` have `SeqNum` 1 and 2, but `bid_ask_data` only has `SeqNum` 1. The highest common ID is 1, so only the `SeqNum` 1 rows appear.
- If `bid_ask_data` were a refreshing table that later received `SeqNum` 2 for `GOOGL`, the filter would replace the `SeqNum` 1 rows with the `SeqNum` 2 rows.

### Refreshing example

This example shows the filter advancing as rows arrive in refreshing tables. Two [time tables](./time-table.md) add rows at different rates, and each new row gets the next `SeqNum` for the `AAPL` key:

```python ticking-table order=null test-set=1
import jpy
from deephaven import time_table
from deephaven.table import Table

SyncTableFilterBuilder = jpy.get_type(
    "io.deephaven.engine.table.impl.util.SyncTableFilter$Builder"
)

fast_feed = time_table("PT1s").update(
    ["Symbol = `AAPL`", "SeqNum = ii", "Price = 150.0 + ii * 0.01"]
)
slow_feed = time_table("PT3s").update(
    ["Symbol = `AAPL`", "SeqNum = ii", "Volume = 1000 + ii"]
)

feed_builder = SyncTableFilterBuilder("SeqNum", "Symbol")
feed_builder.addTable("fast", fast_feed.j_table)
feed_builder.addTable("slow", slow_feed.j_table)

result = feed_builder.build()

synced_fast = Table(result.get("fast"))
synced_slow = Table(result.get("slow"))
```

`fast_feed` adds a row every second and `slow_feed` adds one every three seconds, so `fast_feed` is never behind `slow_feed`. As the tables tick:

- Each synchronized table shows one row: the latest `SeqNum` in `slow_feed`.
- Each time `slow_feed` adds a row, both results drop the previous `SeqNum` row and show the new one.
- The newer rows in `fast_feed` stay hidden until `slow_feed` reaches their `SeqNum`.

### API

Create a builder with the ID column name and key column names:

```python syntax
builder = SyncTableFilterBuilder(id_column, key_column1, key_column2, ...)
```

Add each table with a unique name:

```python syntax
builder.addTable(table_name, table.j_table)
```

When a table's ID or key column names differ from the builder's defaults, pass the ID column and every key column to [`addTable`](/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.Builder.html), including key columns whose names match the defaults. Unlike [`LeaderTableFilter`](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.html), this builder doesn't fill in missing key columns:

```python syntax
builder.addTable(table_name, table.j_table, id_column, key_column1, key_column2, ...)
```

Build and retrieve the synchronized tables:

```python syntax
result = builder.build()
synced_table = Table(result.get(table_name))
```

## `LeaderTableFilter`

[`LeaderTableFilter`](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.html) synchronizes multiple tables using a leader-follower pattern.

### How it works

The leader table has one ID column for each follower table, so each leader row names one ID per follower.

A follower's ID is satisfied when either of these is true:

- The follower table has a row with that ID for the key.
- The ID is null. A null ID contributes no rows from that follower.

For each key, the filter:

- Finds the most recent leader row whose follower IDs are all satisfied.
- Shows that leader row and the matching rows from each follower table.
- Replaces them when a later leader row for the key is fully satisfied.

### Example

This example uses a synchronization log, `sync_log`, as the leader table. Its rows are keyed by `Client` and `Session`, and its `TradeId` and `MessageId` columns name the trade and message to show for each key.

The follower tables use different column names than the leader, so each [`addTable`](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.TableBuilder.html) call names them:

- The ID mapping, such as `"TradeId=Id"`, pairs the leader's ID column with the follower's.
- The key columns, `Client` and `SessionId`, are the follower's names for the leader's `Client` and `Session` columns.

```python order=filtered_leader,filtered_trades,filtered_messages
import jpy
from deephaven import new_table
from deephaven.column import string_col, long_col, double_col
from deephaven.table import Table

LeaderTableFilterBuilder = jpy.get_type(
    "io.deephaven.engine.util.LeaderTableFilter$TableBuilder"
)

sync_log = new_table(
    [
        string_col("Client", ["ClientA", "ClientA", "ClientB"]),
        string_col("Session", ["S1", "S1", "S2"]),
        long_col("TradeId", [100, 101, 200]),
        long_col("MessageId", [1, 2, 5]),
    ]
)

trade_log = new_table(
    [
        string_col("Client", ["ClientA", "ClientA", "ClientA", "ClientB"]),
        string_col("SessionId", ["S1", "S1", "S1", "S2"]),
        long_col("Id", [100, 101, 102, 200]),
        string_col("Symbol", ["AAPL", "GOOGL", "MSFT", "TSLA"]),
        double_col("Quantity", [100.0, 50.0, 75.0, 200.0]),
    ]
)

message_log = new_table(
    [
        string_col("Client", ["ClientA", "ClientA", "ClientA", "ClientB", "ClientB"]),
        string_col("SessionId", ["S1", "S1", "S1", "S2", "S2"]),
        long_col("MsgId", [1, 2, 3, 5, 6]),
        string_col(
            "Message",
            [
                "Order placed",
                "Order filled",
                "Order confirmed",
                "Trade executed",
                "Settlement",
            ],
        ),
    ]
)

builder = LeaderTableFilterBuilder(sync_log.j_table, "Client", "Session")
builder.addTable("trades", trade_log.j_table, "TradeId=Id", "Client", "SessionId")
builder.addTable(
    "messages", message_log.j_table, "MessageId=MsgId", "Client", "SessionId"
)

result = builder.build()

filtered_leader = Table(result.getLeader())
filtered_trades = Table(result.get("trades"))
filtered_messages = Table(result.get("messages"))
```

In this example:

- The `sync_log` leader table controls which trades and messages appear. The filter shows only the most recent fully matched leader row for each key.
- For `ClientA/S1`, the leader has two rows: (`TradeId` 100, `MessageId` 1) and (`TradeId` 101, `MessageId` 2). `trade_log` and `message_log` fully match both. However, only the most recent match appears in the synchronized results. That match is `TradeId` 101 and `MessageId` 2.
- Even though `trade_log` has `Id` 102 and `message_log` has `MsgId` 3, they don't appear because the leader hasn't referenced them yet.
- For `ClientB/S2`, only trade 200 and message 5 appear.

### API

Create a builder with the leader table and key columns:

```python syntax
builder = LeaderTableFilterBuilder(leader_table.j_table, key_column1, key_column2, ...)
```

Add each follower table with:

- A unique name
- The follower table
- An ID column mapping in the form `"leaderIdCol=followerIdCol"`, or a single column name when the leader and follower ID columns have the same name
- The follower's key columns, which must match the leader's key column types

```python syntax
builder.addTable(
    table_name,
    table.j_table,
    "leaderIdCol=followerIdCol",
    follower_key_col1,
    follower_key_col2,
    ...,
)
```

Omit the follower key columns to reuse the leader's key column names.

A leader can have at most 32 follower tables. [`build`](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.TableBuilder.html#build()) throws an `IllegalArgumentException` if you add more.

Build and retrieve the synchronized tables:

```python syntax
result = builder.build()
filtered_leader = Table(result.getLeader())
filtered_follower = Table(result.get(table_name))
```

### Partitioned table variant

When the leader and follower tables are [partitioned tables](./partitioned-tables.md), use [`LeaderTableFilter.PartitionedTableBuilder`](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.PartitionedTableBuilder.html) instead of the table builder. It takes [partitioned tables](./partitioned-tables.md) as input, matches their [constituent tables](./partitioned-tables.md) by partition key, and returns [partitioned tables](./partitioned-tables.md) of filtered constituents.

The key columns you pass to the builder are the columns inside each [constituent table](./partitioned-tables.md) that group related rows, as with the table builder. The partition key columns come from the [partitioned tables](./partitioned-tables.md) themselves, so you don't pass them to the builder.

Access it through [`jpy`](./use-jpy.md):

```python syntax
import jpy
from deephaven.table import PartitionedTable

PartitionedTableBuilder = jpy.get_type(
    "io.deephaven.engine.util.LeaderTableFilter$PartitionedTableBuilder"
)
builder = PartitionedTableBuilder(
    leader_partitioned_table.j_partitioned_table,
    constituent_key_col1,
    constituent_key_col2,
    ...,
)
builder.addPartitionedTable(
    name, follower_partitioned_table.j_partitioned_table, "leaderIdCol=followerIdCol"
)
result = builder.build()

filtered_leader = PartitionedTable(result.getLeader())
filtered_follower = PartitionedTable(result.get(name))
```

The partitioned builder also requires:

- The leader and every follower [partitioned table](./partitioned-tables.md) have unique keys, so each key has one [constituent table](./partitioned-tables.md).
- All [partitioned tables](./partitioned-tables.md) have the same number of partition key columns.
- Partition key columns have the same data types in every [partitioned table](./partitioned-tables.md).
- Partition key columns appear in the same order in every [partitioned table](./partitioned-tables.md), because the filter matches them by position.
- Every [constituent table](./partitioned-tables.md) is [add-only](../conceptual/table-types.md#specialization-2-add-only). The filter checks each constituent once the leader and every follower have a constituent for its partition key, including constituents that arrive after you call [`build`](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.PartitionedTableBuilder.html#build()).

The results contain a constituent for a partition key only once the leader and every follower have a constituent for that key.

## Thread safety

Console and script-session code already runs under the [update graph](../conceptual/table-update-model.md)'s exclusive lock. The examples above call `build` directly, which is safe in the console even when the input tables are refreshing.

If you call one of these builders from an external worker or timer thread, the thread needs two things:

- An [execution context](../conceptual/execution-context.md). Without it, `build` fails when an input table is refreshing.
- The [update graph](../conceptual/table-update-model.md)'s [shared lock](../conceptual/query-engine/engine-locking.md#shared-lock). Without it, `build` throws an `IllegalStateException` when an input table is refreshing.

To provide both:

1. Capture the context with [`get_exec_ctx`](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.get_exec_ctx) on the console thread.
2. Open the context on the worker thread with a `with` statement.
3. Wrap the call in [`auto_locking_ctx`](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.auto_locking_ctx), and pass it the Python [`Table`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table) objects you built the filter from.

[`auto_locking`](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.auto_locking) is on by default. While it's on, [`auto_locking_ctx`](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.auto_locking_ctx) takes the shared lock when one of the tables you pass is refreshing and the thread holds no [update graph](../conceptual/table-update-model.md) lock.

Pass the `Table` objects, not their `j_table` values. A `j_table` value has no `is_refreshing` attribute, so [`auto_locking_ctx`](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.auto_locking_ctx) treats it as static and takes no lock.

This example builds the synchronized tables from the [refreshing example](#refreshing-example) on a worker thread:

```python ticking-table order=null test-set=1
import threading
from deephaven.execution_context import get_exec_ctx
from deephaven.table import Table
from deephaven.update_graph import auto_locking_ctx

# On the console thread
ctx = get_exec_ctx()


def build_on_worker():
    global worker_fast, worker_slow
    # On the worker thread
    with ctx, auto_locking_ctx(fast_feed, slow_feed):
        worker_result = feed_builder.build()
        worker_fast = Table(worker_result.get("fast"))
        worker_slow = Table(worker_result.get("slow"))


threading.Thread(target=build_on_worker).start()
```

When the worker finishes, `worker_fast` and `worker_slow` show the same rows as `synced_fast` and `synced_slow`.

The script starts the worker thread and doesn't wait for it. Console code holds the exclusive lock until the script finishes. If the script waited for the worker, for example with `join`, the worker could never acquire the shared lock.

Don't take the lock inside a [listener callback](./table-listeners-python.md). A [listener callback](./table-listeners-python.md) runs on a thread that processes updates, and [`UpdateGraphLock`](/core/javadoc/io/deephaven/engine/updategraph/UpdateGraphLock.html) rejects lock attempts from those threads. `build` doesn't need the lock there.

Choose the locking call by where your code runs:

- [`auto_locking_ctx`](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.auto_locking_ctx): works on an external thread and inside a [listener callback](./table-listeners-python.md), because it skips the lock in a callback.
- [`shared_lock`](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.shared_lock): works on an external thread only.

See [Update graph locks and thread safety](./table-listeners-python.md#update-graph-locks-and-thread-safety) for more on when explicit locking is needed.

## Related documentation

- [Deephaven's table types](../conceptual/table-types.md)
- [Incremental update model](../conceptual/table-update-model.md)
- [Execution context](../conceptual/execution-context.md)
- [Engine locking](../conceptual/query-engine/engine-locking.md)
- [Filters](./use-filters.md)
- [Partitioned tables](./partitioned-tables.md)
- [Create a time table](./time-table.md)
- [Use jpy](./use-jpy.md)
- [Listen to ticking tables](./table-listeners-python.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`string_col`](../reference/table-operations/create/stringCol.md)
- [`long_col`](../reference/table-operations/create/longCol.md)
- [`double_col`](../reference/table-operations/create/doubleCol.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`update_view`](../reference/table-operations/select/update-view.md)
- [`view`](../reference/table-operations/select/view.md)
- [`select`](../reference/table-operations/select/select.md)
- [`drop_columns`](../reference/table-operations/select/drop-columns.md)
- [`rename_columns`](../reference/table-operations/select/rename-columns.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`Table.assertAddOnly` Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html#assertAddOnly())
- [`auto_locking` Pydoc](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.auto_locking)
- [`auto_locking_ctx` Pydoc](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.auto_locking_ctx)
- [`get_exec_ctx` Pydoc](/core/pydoc/code/deephaven.execution_context.html#deephaven.execution_context.get_exec_ctx)
- [`shared_lock` Pydoc](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.shared_lock)
- [`UpdateGraphLock` Javadoc](/core/javadoc/io/deephaven/engine/updategraph/UpdateGraphLock.html)
- [`SyncTableFilter` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.html)
- [`LeaderTableFilter` Javadoc](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.html)
- [`SyncTableFilter.Builder` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.Builder.html)
- [`LeaderTableFilter.TableBuilder` Javadoc](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.TableBuilder.html)
- [`LeaderTableFilter.PartitionedTableBuilder` Javadoc](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.PartitionedTableBuilder.html)
- [`Table` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table)
- [`PartitionedTable` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTable)
