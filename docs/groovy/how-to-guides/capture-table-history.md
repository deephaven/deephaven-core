---
title: Capture table history with snapshots
---

This guide shows you how to capture the history of [ticking tables](../conceptual/table-types.md).

An [append-only](../conceptual/table-types.md#specialization-1-append-only) table only adds rows at the bottom and never deletes or modifies them, so the table itself is its history. Other ticking tables can also modify or remove rows, or insert rows in the middle, so the table alone doesn't preserve earlier values. By using [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md), you can capture the history of a table, even if it is not append-only. To keep only the latest state of a table each time another table ticks, instead of its full history, see [Reduce the update frequency of ticking tables](./performance/reduce-update-frequency.md).

## Syntax

[`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md) with the [`SnapshotWhenOptions.Flag.HISTORY`](../reference/table-operations/snapshot/snapshot-when.md#snapshotwhenoptions) flag produces an in-memory table that records the history of a source table. Two tables control it:

- The source table is the table you call [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md) on. `snapshotWhen` copies this table into the result.
- The trigger table is the table whose rows cause snapshots. For each row the trigger table adds, [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md) appends a full copy of the source table to the result. When you create the result, `snapshotWhen` also appends one copy for each row the trigger table already has. If the source table is empty when the trigger table adds a row, that trigger row adds nothing to the result.

The trigger table is often a [time table](../reference/table-operations/create/timeTable.md), which adds rows at a regular, user-defined interval. A time table adds its new rows once per [update graph cycle](../conceptual/periodic-update-graph-configuration.md), which targets one second by default. While cycles keep pace with the time table's period, it adds one row per interval, so the result gets one snapshot per interval; if a cycle runs long, the next cycle adds every row that came due in the meantime. A time table created without a start time starts empty and adds exactly one row in the first update graph cycle after you create it. Because the time table has no rows when you create the result, it adds no initial snapshot.

Stamp columns are columns from the trigger table that [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md) adds to every row of a snapshot, so each row shows which trigger event captured it.

- Use `stampColumns` to choose them.
- If you pass no stamp columns, every column of the trigger table is a stamp column.

```groovy syntax
import io.deephaven.api.snapshot.SnapshotWhenOptions

result = source.snapshotWhen(trigger, SnapshotWhenOptions.Flag.HISTORY)
result = source.snapshotWhen(trigger, [SnapshotWhenOptions.Flag.HISTORY], stampColumns...)
result = source.snapshotWhen(trigger, SnapshotWhenOptions.of(false, false, true, stampColumns...))
```

In the last form, the three booleans passed to [`SnapshotWhenOptions.of`](../reference/table-operations/snapshot/snapshot-when.md#snapshotwhenoptions) are `initial`, `incremental`, and `history`, so `of(false, false, true)` turns on history only.

## Requirements and limitations

With the [`HISTORY`](../reference/table-operations/snapshot/snapshot-when.md#snapshotwhenoptions) flag, the trigger table must be append-only. A time table meets this requirement unless you build it as a [blink table](../conceptual/table-types.md#specialization-3-blink) with [`timeTableBuilder`](../how-to-guides/time-table.md#timetable-as-a-blink-table) and `blinkTable(true)`. A blink table keeps only the rows added in the current [update graph cycle](../conceptual/periodic-update-graph-configuration.md). If the trigger table modifies or removes rows, or adds a row anywhere but after its last row, the snapshot raises an assertion error and the result table stops updating.

You can't combine [`HISTORY`](../reference/table-operations/snapshot/snapshot-when.md#snapshotwhenoptions) with the [`INITIAL` or `INCREMENTAL`](../reference/table-operations/snapshot/snapshot-when.md#snapshotwhenoptions) flags.

The result records the source table only at trigger events. If the source table changes more than once between two trigger rows, the result keeps only the state at the next trigger.

> [!CAUTION]
> If the source table has a column with the same name as a stamp column, `snapshotWhen` throws an exception. To avoid this, rename the stamp column, either in the trigger table with [`renameColumns`](../reference/table-operations/select/rename-columns.md) or in `stampColumns` itself (for example, `"TriggerTimestamp = Timestamp"`). You can also pass only the stamp columns you need in `stampColumns`.

> [!CAUTION]
> When you use [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md) to capture full table history, Deephaven stores a copy of the entire source table for every trigger event. A large source table or a frequently ticking trigger table can use a lot of memory.

## Capture the history of a ticking table

The example uses two input tables:

- `source` starts from a [time table](../reference/table-operations/create/timeTable.md) with a 0.2-second period. A time table adds its new rows once per update graph cycle, as described in [Syntax](#syntax). So the time table adds roughly five rows about once per second, and `source` updates at the same rate.
- `trigger` is a time table with a 2-second period. Each row it adds makes [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md) append a snapshot of `source` to `result`.

Both tables have a `Timestamp` column. The example doesn't pass any stamp columns, so every column of `trigger` becomes a stamp column. It renames the `Timestamp` column of `trigger` to `TriggerTimestamp` to avoid the stamp-column name conflict described in [Requirements and limitations](#requirements-and-limitations).

```groovy ticking-table order=null
import io.deephaven.api.snapshot.SnapshotWhenOptions

source = timeTable("PT0.2S")
    .update("X = i % 2 == 0 ? `A` : `B`", "Y = randomInt(0, 100)", "Z = sqrt(Y)")
    .lastBy("X")

trigger = timeTable("PT2S").renameColumns("TriggerTimestamp = Timestamp")

result = source.snapshotWhen(trigger, SnapshotWhenOptions.Flag.HISTORY)
```

Because of [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md), `source` has at most two rows: the latest row for `X = A` and the latest row for `X = B`.

Usually both time tables get their first row in the same update graph cycle. Each time table adds only one row in its first cycle, as described in [Syntax](#syntax), so at that point `source` has only the `X = A` row. The first snapshot usually copies that one row. Every later snapshot appends a copy of both rows. Each copied row carries the `TriggerTimestamp` of the trigger row that captured it.

Existing rows in `result` never change, so `result` keeps a record of `source` as it was at each trigger event.

As described in [Requirements and limitations](#requirements-and-limitations), `result` doesn't record changes to `source` between two trigger events. In this example, `source` updates about once per second, but `trigger` adds a row only every two seconds, so [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md) appends a snapshot only every two seconds.

## Related documentation

- [Create a time table](./time-table.md)
- [Reduce the update frequency of ticking tables](./performance/reduce-update-frequency.md)
- [Table types](../conceptual/table-types.md)
- [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md)
- [`renameColumns`](../reference/table-operations/select/rename-columns.md)
- [`snapshot`](../reference/table-operations/snapshot/snapshot.md)
- [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
