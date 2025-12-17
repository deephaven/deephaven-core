---
title: getAttributes
---

The `getAttributes` method returns all the attributes in the source table's `AttributeMap`.

## Syntax

```groovy syntax
table.getAttributes()
table.getAttributes(included)
```

## Parameters

<ParamTable>
<Param name="included" type="Predicate<String>">

A predicate to determine which attribute keys to include.

</Param>
</ParamTable>

## Returns

All of the attributes in the source table's `AttributeMap`.

## Examples

In this example, we create an [Input Table](../../../how-to-guides/input-tables.md) and then print the result of a call to `getAttributes()`, returning our Input Table's `AttributeMap`.

```groovy order=:log
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
import io.deephaven.engine.table.AttributeMap

source = newTable(
    doubleCol("Doubles", 3.1, 5.45, -1.0),
    stringCol("Strings", "Creating", "New", "Tables")
)

result = AppendOnlyArrayBackedInputTable.make(source)

println result.getAttributes()
```

## Related documentation

- [`withoutAttributes`](../create/withoutAttributes.md)
- [`getAttribute`](./getAttribute.md)
- [`getAttributeKeys`](./getAttributeKeys.md)
- [`hasAttribute`](./hasAttribute.md)
- [`retainingAttributes`](../select/retainingAttributes.md)
- [`withAttributes`](../select/withAttributes.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/AttributeMap.html#getAttributes())
