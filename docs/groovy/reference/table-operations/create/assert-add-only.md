---
title: assertAddOnly
---

The `assertAddOnly` method returns a copy of the source table or the source table itself if it is already an add-only table, with the [add-only attribute](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#ADD_ONLY_TABLE_ATTRIBUTE) set.

The source table's update pattern must already conform to add-only semantics. If it produces an update that does not conform to add-only semantics, then the returned table will notify of an error and cease updating.

If the engine can identify a table as add only, then some query operations may be optimized (for example, a lastBy operation need only track the current last row per-group rather than all of the rows in a group). In formulas, the `k` variable (for the current row key) can be used safely.

## Syntax

```groovy syntax
table.assertAddOnly()
```

## Parameters

This method takes no arguments.

## Returns

A new Table with the add-only attribute set.

## Related documentation

- [assertBlink](assert-blink.md)
- [assertAppendOnly](assert-append-only.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#assertAddOnly())
