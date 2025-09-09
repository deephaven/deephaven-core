---
title: Create a ring table
sidebar_label: Ring tables
---

This guide will show you how to create a [ring table](../conceptual/table-types.md#specialization-4-ring) from a [blink table](../conceptual/table-types.md#specialization-3-blink) or an [append-only table](../conceptual/table-types.md#specialization-1-append-only).

Ring tables are much less memory-intensive than append-only tables. While append-only tables can grow infinitely in size, ring tables only hold on to as many rows as the user tells them to - once a row passes out of that range, it is eligible to be deleted from memory.

Ring tables are mostly used with blink tables, which do not retain their own data for more than an update cycle. For example, a blink table only stores rows from the current update cycle, but could be converted to a ring table that preserves the last 5000 rows instead.

## Examples

In this example, we'll create a ring table with a 3-row capacity from a simple append-only time table.

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.sources.ring.RingTableTools

source = timeTable("PT00:00:01")
result = RingTableTools.of(source, 3)
```

![Animated GIF showing a ring table with 3-row capacity where only the most recent three timestamps are kept](../assets/how-to/ring-table-1.gif)

A more common use case is to create a ring table from a blink table to preserve some data history. The following example creates a ring table from a blink time table. In `source`, old data is removed from memory as soon as new data enters the table. In `result`, 5 rows are kept, which preserves 4 more rows than `source`.

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.sources.ring.RingTableTools

source = timeTable("PT00:00:01").update("X = i")
result = RingTableTools.of(source, 5)
```

![Animated GIF comparing blink source table (single current row) to ring table preserving the last five rows](../assets/how-to/ring-table.gif)

The following example creates a ring table from a time table that starts with 5 rows. The `initialize` argument is not set, and so is `True` by default. This means the ring table initially starts with all 5 rows populated.

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.sources.ring.RingTableTools

staticSource = emptyTable(5).update("X = i")
dynamicSource = timeTable("PT00:00:01").update("X = i + 5").dropColumns("Timestamp")
source = merge(staticSource, dynamicSource)
result = RingTableTools.of(source, 5)
```

![Animated GIF of ring table initialized with five rows and rolling forward to always show the latest five](../assets/how-to/ring-table-2.gif)

The following example is identical to the one above, except `initialize` is set to `False`. Thus, when the query is first run, `result` is empty.

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.sources.ring.RingTableTools

staticSource = emptyTable(5).update("X = i")
dynamicSource = timeTable("PT00:00:01").update("X = i + 5").dropColumns("Timestamp")
source = merge(staticSource, dynamicSource)
result = RingTableTools.of(source, 5, false)
```

![The above `source` and `result` tables](../assets/how-to/ring-table-3.gif)

## Related documentation

- [How to create an empty table](../how-to-guides/new-and-empty-table.md#emptytable)
- [How to create a time table](./time-table.md)
- [Table types](../conceptual/table-types.md)
- [`dropColumns`](../reference/table-operations/select/drop-columns.md)
- [`merge`](../reference/table-operations/merge/merge.md)
- [`tail`](../reference/table-operations/filter/tail.md)
- [`update`](../reference/table-operations/select/update.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/sources/ring/RingTableTools.html)
