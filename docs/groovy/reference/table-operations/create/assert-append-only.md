---
title: assertAppendOnly
---

The `assertAppendOnly` method returns a copy of the source table or the source table itself if it is already an append-only table, with the [append-only attribute](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#APPEND_ONLY_TABLE_ATTRIBUTE) set.

The source table's update pattern must already conform to append-only semantics. If it produces an update that does not conform to append-only semantics, then the returned table will notify of an error and cease updating.

If the engine can identify a table as append only, then some query operations may be optimized (for example, a lastBy operation need only track the current last row per-group rather than all of the rows in a group). In formulas, the `i` (for the current row position), `ii` (for the current row position), and `k` (for the current row key) variables can be used safely.

## Syntax

```groovy syntax
table.assertAppendOnly()
```

## Parameters

This method takes no arguments.

## Returns

A new Table with the append-only attribute set.

## Related documentation

- [assertBlink](assert-blink.md)
- [assertAddOnly](assert-add-only.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#assertAppendOnly())
