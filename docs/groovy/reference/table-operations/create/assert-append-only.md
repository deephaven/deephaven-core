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

## Example

The following example creates a simple table using `timeTable` and asserts that it is append-only. This enables optimizations and safe use of the `i`, `ii`, and `k` variables in formulas.

```groovy skip-test
import io.deephaven.engine.table.impl.TimeTable.Builder

builder = new Builder().period("PT1S")
t = builder.build()

// Assert that it is append-only to enable optimizations
tAppendOnly = t.assertAppendOnly()

// Verify the attribute is set
println(tAppendOnly.isAppendOnly())
```

## Related documentation

- [assertBlink](assert-blink.md)
- [assertAddOnly](assert-add-only.md)
- [Javadoc](<https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#assertAppendOnly()>)
