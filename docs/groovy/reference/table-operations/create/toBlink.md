---
title: toBlink
---

The Deephaven `toBlink` method converts an add-only or append-only table to a blink table.

## Syntax

```
AddOnlyToBlinkTableAdapter.toBlink(table)
```

## Parameters

<ParamTable>
<Param name="table>" type="Table">

The add-only or append-only table to convert.

</Param>
</ParamTable>

## Returns

A blink table.

## Examples

```groovy order=result,source
import io.deephaven.engine.table.impl.AddOnlyToBlinkTableAdapter

// create source table
source = timeTable("PT1S")

// check `source` table's attributes
println source.getAttributes()

// convert `source` table to a blink table
result = AddOnlyToBlinkTableAdapter.toBlink(source)

// confirm `result` is now a blink table
println result.getAttributes()
```

## Related documentation

- [`emptyTable`](./timeTable.md)
- [`timeTable`](./timeTable.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/AddOnlyToBlinkTableAdapter.html)
