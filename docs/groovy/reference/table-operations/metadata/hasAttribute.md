---
title: hasAttribute
---

The `hasAttribute` method checks if the specified method exists in the source table's `AttributeMap`.

## Syntax

```groovy syntax
table.hasAttribute(key)
```

## Parameters

<ParamTable>
<Param name="key>" type="String">

The name of the attribute.

</Param>
</ParamTable>

## Returns

A Boolean that is `True` if the specified attribute exists in the source table's `AttributeMap`, or `False` if it does not.

## Examples

In this example, we create an [Input Table](../../../how-to-guides/input-tables.md) and then print the result of a call to `hasAttribute()`.

```groovy order=:log
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
import io.deephaven.engine.table.AttributeMap

source = newTable(
    doubleCol("Doubles", 3.1, 5.45, -1.0),
    stringCol("Strings", "Creating", "New", "Tables")
)

result = AppendOnlyArrayBackedInputTable.make(source)

println result.hasAttribute("AddOnly")
```

## Related documentation

- [`withoutAttributes`](../create/withoutAttributes.md)
- [`getAttribute`](./getAttribute.md)
- [`getAttributeKeys`](./getAttributeKeys.md)
- [`getAttributes`](./getAttributes.md)
- [`retainingAttributes`](../select/retainingAttributes.md)
- [`withAttributes`](../select/withAttributes.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/AttributeMap.html#hasAttribute(java.lang.String))
