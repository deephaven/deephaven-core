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

```groovy order=null
import io.deephaven.engine.table.impl.util.SyncTableFilter

priceData = newTable(
    stringCol("Symbol", "AAPL", "AAPL", "AAPL", "GOOGL", "GOOGL"),
    longCol("SeqNum", 1, 2, 3, 1, 2),
    doubleCol("Price", 150.0, 151.0, 152.0, 2800.0, 2805.0)
)

volumeData = newTable(
    stringCol("Symbol", "AAPL", "AAPL", "GOOGL", "GOOGL"),
    longCol("SeqNum", 1, 2, 1, 2),
    longCol("Volume", 1000000, 1100000, 500000, 520000)
)

bidAskData = newTable(
    stringCol("Symbol", "AAPL", "AAPL", "GOOGL"),
    longCol("SeqNum", 1, 2, 1),
    doubleCol("Bid", 149.95, 150.95, 2799.50),
    doubleCol("Ask", 150.05, 151.05, 2800.50)
)

builder = new SyncTableFilter.Builder("SeqNum", "Symbol")
builder.addTable("prices", priceData)
builder.addTable("volumes", volumeData)
builder.addTable("bidAsk", bidAskData)

result = builder.build()

syncedPrices = result.get("prices")
syncedVolumes = result.get("volumes")
syncedBidAsk = result.get("bidAsk")
```

In this example:

- For `AAPL`, all three tables have `SeqNum` 1 and 2, so those rows appear in the synchronized results.
- For `AAPL`, only `priceData` has `SeqNum` 3, so that row is filtered out.
- For `GOOGL`, only `bidAskData` is missing `SeqNum` 2, so only rows with `SeqNum` 1 appear.
- When `bidAskData` receives `SeqNum` 2 for `GOOGL`, the filter will advance to show those rows.

### API

Create a builder with the ID column name and key column names:

```groovy syntax
builder = new SyncTableFilter.Builder(idColumn, keyColumn1, keyColumn2, ...)
```

Add each table with a unique name:

```groovy syntax
builder.addTable(tableName, table)
```

Build and retrieve the synchronized tables:

```groovy syntax
result = builder.build()
syncedTable = result.get(tableName)
```

## `LeaderTableFilter`

`LeaderTableFilter` synchronizes multiple tables using a leader-follower pattern. The leader table contains ID columns that specify which rows from each follower table to show.

### How it works

The leader table contains one ID column for each follower table. When the leader table has a row with specific ID values, the filter shows the corresponding rows from each follower table that match those IDs.

### Example

This example uses a synchronization log as the leader table:

```groovy order=null
import io.deephaven.engine.util.LeaderTableFilter

syncLog = newTable(
    stringCol("Client", "ClientA", "ClientA", "ClientB"),
    stringCol("Session", "S1", "S1", "S2"),
    longCol("TradeId", 100, 101, 200),
    longCol("MessageId", 1, 2, 5)
)

tradeLog = newTable(
    stringCol("Client", "ClientA", "ClientA", "ClientA", "ClientB"),
    stringCol("SessionId", "S1", "S1", "S1", "S2"),
    longCol("Id", 100, 101, 102, 200),
    stringCol("Symbol", "AAPL", "GOOGL", "MSFT", "TSLA"),
    doubleCol("Quantity", 100.0, 50.0, 75.0, 200.0)
)

messageLog = newTable(
    stringCol("Client", "ClientA", "ClientA", "ClientA", "ClientB", "ClientB"),
    stringCol("SessionId", "S1", "S1", "S1", "S2", "S2"),
    longCol("MsgId", 1, 2, 3, 5, 6),
    stringCol("Message", "Order placed", "Order filled", "Order confirmed", "Trade executed", "Settlement")
)

builder = new LeaderTableFilter.TableBuilder(syncLog, "Client", "Session")
builder.addTable("trades", tradeLog, "TradeId=Id", "Client", "SessionId")
builder.addTable("messages", messageLog, "MessageId=MsgId", "Client", "SessionId")

result = builder.build()

filteredLeader = result.getLeader()
filteredTrades = result.get("trades")
filteredMessages = result.get("messages")
```

In this example:

- The `syncLog` leader table controls which trades and messages appear.
- For `ClientA/S1`, the leader shows `TradeId` 100 and 101, and `MessageId` 1 and 2.
- Even though `tradeLog` has `Id` 102 and `messageLog` has `MsgId` 3, they don't appear because the leader hasn't referenced them yet.
- For `ClientB/S2`, only trade 200 and message 5 appear.

### API

Create a builder with the leader table and key columns:

```groovy syntax
builder = new LeaderTableFilter.TableBuilder(leaderTable, keyColumn1, keyColumn2, ...)
```

Add each follower table with:

- A unique name
- The table reference
- ID column mapping (format: `"leaderIdColumn=followerIdColumn"`)
- Key columns in the follower table (must match leader key columns in type)

```groovy syntax
builder.addTable(tableName, table, "leaderIdCol=followerIdCol", followerKeyCol1, followerKeyCol2, ...)
```

Build and retrieve the synchronized tables:

```groovy syntax
result = builder.build()
filteredLeader = result.getLeader()
filteredFollower = result.get(tableName)
```

### Partitioned table variant

`LeaderTableFilter.PartitionedTableBuilder` works with partitioned tables:

```groovy syntax
builder = new LeaderTableFilter.PartitionedTableBuilder(leaderPartitionedTable)
builder.addTable(name, followerPartitionedTable, "leaderIdCol=followerIdCol")
result = builder.build()
```

Requirements:

- All partitioned tables have the same number of key columns.
- Key columns have compatible types.
- Key columns are joined in order.
- Constituent tables within each partition are add-only.

## Related documentation

- [Filters](./filters.md)
- [Partitioned tables](./partitioned-tables.md)
- [`SyncTableFilter` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/SyncTableFilter.html)
- [`LeaderTableFilter` Javadoc](/core/javadoc/io/deephaven/engine/util/LeaderTableFilter.html)
