---
title: isBlink
---

The `isBlink` method returns `true` if the table is a blink table, and `false` otherwise.

Blink tables keep only the set of rows received during the current update cycle. The table only consists of rows added in the previous update cycle, and no rows persist for more than one update cycle.

## Syntax

```groovy syntax
table.isBlink()
```

## Parameters

This method takes no arguments.

## Returns

A `boolean` value: `true` if the table is a blink table, `false` otherwise.

## Examples

The following example demonstrates checking a table before and after converting it to a blink table.

```groovy ticking-table order=:log
import io.deephaven.engine.table.impl.AddOnlyToBlinkTableAdapter

source = timeTable("PT1S")

println "Before conversion: ${source.isBlink()}"

result = AddOnlyToBlinkTableAdapter.toBlink(source)

println "After conversion: ${result.isBlink()}"
```

## Related documentation

- [`timeTable`](../create/timeTable.md)
- [`toBlink`](../create/toBlink.md)
- [`removeBlink`](../create/remove-blink.md)
- [`blinkToAppendOnly`](../create/blink-to-append-only.md)
- [Table types: Specialized semantics for blink tables](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/BaseTable.html#isBlink())
