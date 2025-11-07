---
title: withAttributes
---

The `withAttributes` method returns a table with an `AttributeMap` identical to the source table's, but with the specified attributes added/replaced. If the supplied attributes `toAdd` would not result in any changes, the original object may be returned.

## Syntax

```groovy syntax
withAttributes(toAdd)
withAttributes(toAdd, toRemove)
```

## Parameters

<ParamTable>
<Param name="toAdd>" type="Map<String,Object>">

Attribute key-value pairs to add or replace (if the key already exists in the source table's `AttributeMap`). Neither keys nor values may be null.

</Param>
<Param name="toRemove>" type="Collection<String>">

Attribute keys to remove.

</Param>
</ParamTable>

## Returns

A table with an `AttributeMap` that is the same as the source table's, but with the specified attributes added or removed.

## Examples

In this example, we create a source table, then create a `result` table with a call to `withAttributes()`. This returns a `result` table that is identical to our source table, but with the attribute we specify (`"MyAttribute"`) added.

```groovy order=source,result
import io.deephaven.engine.table.AttributeMap

source = newTable(
    doubleCol("Doubles", 3.1, 5.45, -1.0),
    stringCol("Strings", "Creating", "New", "Tables")
)

result = source.withAttributes(Map.of("MyAttribute", "MyBooleanValue"))

println source.getAttributeKeys()
println result.getAttributeKeys()
```

## Related documentation

- [`getAttribute`](../metadata/getAttribute.md)
- [`withoutAttributes`](../create/withoutAttributes.md)
- [`getAttributeKeys`](../metadata//getAttributeKeys.md)
- [`getAttributes`](../metadata/getAttributes.md)
- [`hasAttribute`](../metadata/hasAttribute.md)
- [`retainingAttributes`](../select/retainingAttributes.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/AttributeMap.html#withAttributes(java.util.Map))
