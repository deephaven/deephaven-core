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

`SyncTableFilter` synchronizes multiple peer tables by showing only rows where all tables have the same minimum ID for each key.

### How it works

For each key, the filter identifies the minimum ID value across all input tables. Only rows with that minimum ID are passed through. When all tables advance to the next ID, the filter removes the old ID's rows and adds the new ID's rows.

### Example

This example synchronizes three tables that share `Symbol` as a key and use `SeqNum` as the ID:

```python order=null
import jpy
from deephaven import new_table
from deephaven.column import string_col, long_col, double_col

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

synced_prices = result.get("prices")
synced_volumes = result.get("volumes")
synced_bid_ask = result.get("bidAsk")
```

In this example:

- For `AAPL`, all three tables have `SeqNum` 1 and 2, so those rows appear in the synchronized results.
- For `AAPL`, only `price_data` has `SeqNum` 3, so that row is filtered out.
- For `GOOGL`, only `bid_ask_data` is missing `SeqNum` 2, so only rows with `SeqNum` 1 appear.
- When `bid_ask_data` receives `SeqNum` 2 for `GOOGL`, the filter will advance to show those rows.

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
synced_table = result.get(table_name)
```

## `LeaderTableFilter`

`LeaderTableFilter` synchronizes multiple tables using a leader-follower pattern. The leader table contains ID columns that specify which rows from each follower table to show.

### How it works

The leader table contains one ID column for each follower table. When the leader table has a row with specific ID values, the filter shows the corresponding rows from each follower table that match those IDs.

### Example

This example uses a synchronization log as the leader table:

```python order=null
import jpy
from deephaven import new_table
from deephaven.column import string_col, long_col, double_col

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

filtered_leader = result.getLeader()
filtered_trades = result.get("trades")
filtered_messages = result.get("messages")
```

In this example:

- The `sync_log` leader table controls which trades and messages appear.
- For `ClientA/S1`, the leader shows `TradeId` 100 and 101, and `MessageId` 1 and 2.
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
filtered_leader = result.getLeader()
filtered_follower = result.get(table_name)
```

### Partitioned table variant

`LeaderTableFilter.PartitionedTableBuilder` works with partitioned tables. Access it via jpy:

```python syntax
PartitionedTableBuilder = jpy.get_type(
    "io.deephaven.engine.util.LeaderTableFilter$PartitionedTableBuilder"
)
builder = PartitionedTableBuilder(leader_partitioned_table.j_partitioned_table)
builder.addTable(
    name, follower_partitioned_table.j_partitioned_table, "leaderIdCol=followerIdCol"
)
result = builder.build()
```

Requirements:

- All partitioned tables must have the same number of key columns.
- Key columns must have compatible types.
- Key columns are joined in order.
- Constituent tables within each partition must be add-only.

## Related documentation

- [Filters](./use-filters.md)
- [Partitioned tables](./partitioned-tables.md)
- [`SyncTableFilter` Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.html)
- [`LeaderTableFilter` Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.html)
