---
title: getAttributeKeys
---

The `getAttributeKeys` method returns an immutable set of all the attributes that have values in the source table's `AttributeMap`.

## Syntax

```groovy syntax
table.getAttributeKeys()
```

## Parameters

This method has no parameters.

## Returns

An immutable set of all the attributes that have values in the source table's `AttributeMap`.

## Examples

```groovy order=:log
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
import io.deephaven.engine.table.AttributeMap

source = newTable(
    doubleCol("Doubles", 3.1, 5.45, -1.0),
    stringCol("Strings", "Creating", "New", "Tables")
)

result = AppendOnlyArrayBackedInputTable.make(source)

println source.getAttributeKeys()
println result.getAttributeKeys()
```

## Related documentation

- [`withoutAttributes`](../create/withoutAttributes.md)
- [`getAttribute`](./getAttribute.md)
- [`getAttributes`](./getAttributes.md)
- [`hasAttribute`](./hasAttribute.md)
- [`retainingAttributes`](../select/retainingAttributes.md)
- [`withAttributes`](../select/withAttributes.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/AttributeMap.html#getAttributeKeys())
