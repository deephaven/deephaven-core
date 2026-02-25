---
title: retainingAttributes
---

The `retainingAttributes` method returns a table that is the same as the source table, but with only the specified attributes retained. If the supplied attributes `toRetain` do not result in any changes, the original object may be returned.

## Syntax

```groovy syntax
retainingAttributes(toRetain)
```

## Parameters

<ParamTable>
<Param name="toRetain>" type="Collection<String>">

The keys of attributes to retain.

</Param>
</ParamTable>

## Returns

A table that is the same as the source table, but with only the specified attributes retained.

## Examples

In this example, we create an [Input Table](../../../how-to-guides/input-tables.md), and then create a `result` table with a call to `retainAttributes`, which returns a new table with only the specified attributes retained.

```groovy order=:log
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
import io.deephaven.engine.table.AttributeMap

table = newTable(
    doubleCol("Doubles", 3.1, 5.45, -1.0),
    stringCol("Strings", "Creating", "New", "Tables")
)

source = AppendOnlyArrayBackedInputTable.make(table)

result = source.retainingAttributes(["AddOnly"])

println source.getAttributeKeys()
println result.getAttributeKeys()
```

## Related documentation

- [`getAttribute`](../metadata/getAttribute.md)
- [`withoutAttributes`](../create/withoutAttributes.md)
- [`getAttributeKeys`](../metadata/getAttributeKeys.md)
- [`getAttributes`](../metadata/getAttributes.md)
- [`hasAttribute`](../metadata/hasAttribute.md)
- [`withAttributes`](../select/withAttributes.md)
- [Javadoc](<https://deephaven.io/core/javadoc/io/deephaven/engine/table/AttributeMap.html#retainingAttributes(java.util.Collection)>)
