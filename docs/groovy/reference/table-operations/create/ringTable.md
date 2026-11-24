---
title: RingTableTools.of
---

The `RingTableTools.of` method creates a [ring table](../../../conceptual/table-types.md#specialization-4-ring) that retains the latest `capacity` number of rows from the parent table.
Only rows added to the parent table count toward the latest rows.

Ring tables are most often used with [blink tables](../../../conceptual/table-types.md#specialization-3-blink), which retain rows for only one [update cycle](../../../conceptual/table-update-model.md). A ring table retains those rows across update cycles.

## Syntax

```groovy syntax
RingTableTools.of(parent, capacity)
RingTableTools.of(parent, capacity, initialize)
```

[`RingTableTools.of2`](/core/javadoc/io/deephaven/engine/table/impl/sources/ring/RingTableTools.html#of2(io.deephaven.engine.table.Table,int,boolean)) takes the same three arguments as the three-argument `of`, all required. It builds a ring whose size is `capacity` rounded up to a power of 2 and, if that differs from `capacity`, applies [`tail(capacity)`](../filter/tail.md). This can speed up random access to the ring table's data.

## Parameters

<ParamTable>
<Param name="parent" type="Table">

The parent table. The ring table ignores rows removed from the parent table. If the parent table modifies existing rows or shifts them to different row keys, the ring table fails with an error during that update cycle. Use a blink or append-only parent table.

</Param>
<Param name="capacity" type="int">

The maximum number of rows the ring table retains. When more rows are added to the parent table, the ring table drops the oldest rows so that only the latest `capacity` rows remain. `capacity` must be greater than 0.

</Param>
<Param name="initialize" type="boolean">

Whether to initialize the ring table with a snapshot of the parent table. If omitted, defaults to `true`. If the parent table is static and `initialize` is `false`, `RingTableTools.of` throws an `IllegalArgumentException`.

</Param>
</ParamTable>

## Returns

An in-memory ring table. The ring table refreshes only if the parent table refreshes.

## Examples

The following example creates a source table with 5 rows, then creates a ring table with a capacity of 3. `result` holds only the last 3 rows of `source`.

```groovy order=source,result
import io.deephaven.engine.table.impl.sources.ring.RingTableTools

source = newTable(
  intCol("IntegerColumn", 1, 2, 3, 4, 5)
)

result = RingTableTools.of(source, 3)
```

The following example creates a ring table with a capacity of 5 from a blink [time table](./timeTable.md). `source` holds only the rows added in the latest update cycle, while `result` holds up to the 5 most recent rows.

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.sources.ring.RingTableTools

source = timeTableBuilder().period("PT1S").blinkTable(true).build()

result = RingTableTools.of(source, 5)
```

The following example creates two ring tables from a parent table that starts with 5 rows and then gains a new row every second. `resultInitialized` omits `initialize`, which defaults to `true`, so it starts with those 5 rows. `resultEmpty` sets `initialize` to `false`, so it starts empty and holds only rows added to `source` afterward.

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.sources.ring.RingTableTools

staticSource = emptyTable(5).update("X = i")
dynamicSource = timeTable("PT1S").update("X = i + 5").dropColumns("Timestamp")
source = merge(staticSource, dynamicSource)

resultInitialized = RingTableTools.of(source, 10)
resultEmpty = RingTableTools.of(source, 10, false)
```

## Related documentation

- [Create a ring table](../../../how-to-guides/ring-table.md)
- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [Table types](../../../conceptual/table-types.md#specialization-4-ring)
- [Incremental update model](../../../conceptual/table-update-model.md)
- [`emptyTable`](./emptyTable.md)
- [`intCol`](./intCol.md)
- [`merge`](../merge/merge.md)
- [`newTable`](./newTable.md)
- [`tail`](../filter/tail.md)
- [`timeTable`](./timeTable.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/sources/ring/RingTableTools.html#of(io.deephaven.engine.table.Table,int,boolean))
- [`RingTableTools.of2` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/sources/ring/RingTableTools.html#of2(io.deephaven.engine.table.Table,int,boolean))
