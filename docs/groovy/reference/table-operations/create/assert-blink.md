---
title: assertBlink
---

The `assertBlink` method returns a copy of the source table or the source table itself if it is already a blink table, with the [blink attribute](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#BLINK_TABLE_ATTRIBUTE) set.

The source table's update pattern must already conform to blink semantics. If it produces an update that does not conform to blink semantics, then the returned table will notify of an error and cease updating.

This is most useful in conjunction with the [`removeBlink`](remove-blink.md) operation. If you've performed an operation that does not permit blink inputs, but does provide blink semantics on the output; then `assertBlink` adds the attribute back; but care must be taken to ensure that the update pattern of the table does indeed conform to blink semantics.

## Syntax

```groovy syntax
table.assertBlink()
```

## Parameters

This method takes no arguments.

## Returns

A new Table with the blink attribute set.

## Example

The following example creates a simple blink table with the time table builder and then calls `removeBlink` to make it a non-blink table. The non-blink table can be partitioned. The group operation does not produce a blink pattern, because rows may be modified. If the result of `groupBy` was used directly as the source of `assertBlink`, then the resulting table would raise an AssertionFailure during update. Using the `TickSuppressor.convertModificationsToAddsAndRemoves` forces the table to conform to blink semantics (no shifts are present in the result of this aggregation), therefore the result can be made blink again with the assertBlink call.

```groovy order=tNoBlink,tBlink
import io.deephaven.engine.table.impl.TimeTable.Builder
import io.deephaven.engine.table.impl.BlinkTableTools
import io.deephaven.engine.util.TickSuppressor

builder = new Builder().period("PT2S").blinkTable(true)

tBlink = builder.build()
tNoBlink = tBlink.removeBlink()
grouped = TickSuppressor.convertModificationsToAddsAndRemoves(tNoBlink.groupBy())
blinkAgain = grouped.assertBlink()

// the original table is blink
println(tBlink.isBlink())
// but tNoBlink removes the attribute, even though it conforms to the update pattern
println(tNoBlink.isBlink())
// the grouped table is also not a blink table, even though it conforms to the update pattern
println(grouped.isBlink())
// but blinkAgain is a blink table
println(blinkAgain.isBlink())
```

## Related documentation

- [removeBlink](remove-blink.md)
- [assertAddOnly](assert-add-only.md)
- [assertAppendOnly](assert-append-only.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#assertBlink())
