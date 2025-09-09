---
title: snapshot
slug: ./snapshot
---

`snapshot` produces a static, in-memory copy of a source table.

## Syntax

```
result = source.snapshot()
```

## Returns

A static copy of a source table.

## Examples

In the following example, the `source` table updates every second with new data. After some time, a snapshot is taken.

```groovy ticking-table order=null
source = timeTable("PT00:00:01")

// Some time later...
result = source.snapshot()
```

![A user navigates between the ticking `source` table and the `result` snapshot](../../../assets/reference/table-operations/snapshot.gif)

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to capture the history of ticking tables](../../../how-to-guides/capture-table-history.md)
- [How to reduce the update frequency of ticking tables](../../../how-to-guides/reduce-update-frequency.md)
- [`snapshotWhen`](./snapshot-when.md)
- [`timeTable`](../create/timeTable.md)
- [`update`](../select/update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html#snapshot())
