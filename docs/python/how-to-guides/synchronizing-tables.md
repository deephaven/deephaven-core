---
id: synchronizing-tables
title: Synchronize multiple tables
---

When working with multiple related tables in Deephaven, you may encounter situations where tables receive updates at different rates. This guide shows how to use `SyncTableFilter` and `LeaderTableFilter` to coordinate updates across multiple tables.

## The synchronization problem

Deephaven does not provide cross-table or cross-partition transactions. The system processes each partition independently to maximize throughput. This means that a row created later in one partition may appear in your query before a row created earlier in a different partition.

This independence can cause consistency issues when you have multiple tables that contain correlated data. For example:

- A trading system might have separate tables for orders, executions, and messages that all share a common transaction ID.
- A data pipeline might split events across multiple tables, each tagged with a sequence number.
- A multi-source system might need to wait until all sources report data for a given timestamp.

Both `SyncTableFilter` and `LeaderTableFilter` solve this problem by ensuring that only coordinated rows appear in the filtered results.

> [!NOTE]
> These filters are accessed directly through [`jpy`](./use-jpy.md) rather than through a Python API wrapper. Console and script-session code already runs under the update graph's exclusive lock, so calling a builder's `build` method directly, as shown below, is safe there even when the input tables are refreshing. If you call one of these builders from an external worker or timer thread instead, wrap the call in [`auto_locking_ctx`](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.auto_locking_ctx), passing it the builder's input tables as arguments; it only acquires the lock when one of those arguments is actually refreshing. Avoid [`shared_lock`](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.shared_lock) for this: unlike `auto_locking_ctx`, it always attempts to acquire the lock, and `UpdateGraphLock` rejects that attempt if the calling thread is already processing updates, such as inside a listener callback. See [Update graph locks and thread safety](./table-listeners-python.md#update-graph-locks-and-thread-safety) for more on when explicit locking is needed.

## When to use each utility

Choose the synchronization utility based on your table relationships:

- **Use `SyncTableFilter`** when all tables are peers. Each table contributes equally to determining which rows to show. The filter passes through rows where all tables have matching ID values.

- **Use `LeaderTableFilter`** when one table should control synchronization. The leader table contains ID values that dictate which rows from follower tables to show. This is useful when one table acts as a coordination log or contains the authoritative sequence of events.

## Requirements

Both utilities require:

- **Add-only tables**: Tables must not modify, shift, or remove rows. If you filter an add-only source table, the result remains add-only.
- **Monotonically increasing IDs**: ID values must increase for each key. IDs cannot decrease or repeat.
- **Atomic updates**: All rows for a given ID of a given table must appear in the same update.
- **Shared keys**: Tables must have common key columns for grouping.

## `SyncTableFilter`

`SyncTableFilter` synchronizes multiple peer tables by showing, for each key, only the rows at the highest ID that all tables currently share.

### How it works

For each key, the filter finds the highest ID for which every input table has a matching row, and passes through only the rows at that ID. When the tables receive new data and reach a higher commonly available ID, the filter removes the previous ID's rows and adds the new ID's rows.

### Example

This example synchronizes three tables that share `Symbol` as a key and use `SeqNum` as the ID:

```python order=null
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
- When `bid_ask_data` receives `SeqNum` 2 for `GOOGL`, the filter advances to show those rows instead, replacing the `SeqNum` 1 rows.

### API

Create a builder with the ID column name and key column names:

```python syntax
builder = SyncTableFilterBuilder(id_column, key_column1, key_column2, ...)
```

Add each table with a unique name:

```python syntax
builder.addTable(table_name, table.j_table)
```

Build and retrieve the synchronized tables:

```python syntax
result = builder.build()
synced_table = Table(result.get(table_name))
```

## `LeaderTableFilter`

`LeaderTableFilter` synchronizes multiple tables using a leader-follower pattern. The leader table contains ID columns that specify which rows from each follower table to show.

### How it works

The leader table contains one ID column for each follower table. For each key, the filter shows the rows from each follower table that match the IDs in the leader's most recent row for that key, once every follower's ID is satisfied. An ID is satisfied either by a matching row in that follower table, or by a null, which is always treated as satisfied but yields no rows for that follower. An earlier leader row for that key is superseded once a later one is fully satisfied.

### Example

This example uses a synchronization log as the leader table:

```python order=null
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

- The `sync_log` leader table controls which trades and messages appear. Only the most recent leader row per key is shown once its IDs are matched in every follower table.
- For `ClientA/S1`, the leader has two rows: (`TradeId` 100, `MessageId` 1) and (`TradeId` 101, `MessageId` 2). Both are fully matched by `trade_log` and `message_log`. However, only the most recent match, `TradeId` 101 and `MessageId` 2, appears in the synchronized results.
- Even though `trade_log` has `Id` 102 and `message_log` has `MsgId` 3, they don't appear because the leader hasn't referenced them yet.
- For `ClientB/S2`, only trade 200 and message 5 appear.

### API

Create a builder with the leader table and key columns:

```python syntax
builder = LeaderTableFilterBuilder(leader_table.j_table, key_column1, key_column2, ...)
```

Add each follower table with:

- A unique name
- The table reference
- ID column mapping (format: `"leaderIdColumn=followerIdColumn"`)
- Key columns in the follower table (must match leader key columns in type)

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

Build and retrieve the synchronized tables:

```python syntax
result = builder.build()
filtered_leader = Table(result.getLeader())
filtered_follower = Table(result.get(table_name))
```

### Partitioned table variant

`LeaderTableFilter.PartitionedTableBuilder` works with partitioned tables. Access it via jpy:

```python syntax
from deephaven.table import PartitionedTable

PartitionedTableBuilder = jpy.get_type(
    "io.deephaven.engine.util.LeaderTableFilter$PartitionedTableBuilder"
)
builder = PartitionedTableBuilder(
    leader_partitioned_table.j_partitioned_table, key_column1, key_column2, ...
)
builder.addPartitionedTable(
    name, follower_partitioned_table.j_partitioned_table, "leaderIdCol=followerIdCol"
)
result = builder.build()

filtered_leader = PartitionedTable(result.getLeader())
filtered_follower = PartitionedTable(result.get(name))
```

Requirements:

- All partitioned tables have the same number of key columns.
- Key columns have compatible types.
- Key columns are joined in order.
- Constituent tables within each partition are add-only.

## Related documentation

- [Filters](./use-filters.md)
- [Partitioned tables](./partitioned-tables.md)
- [`SyncTableFilter` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.html)
- [`LeaderTableFilter` Javadoc](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.html)
