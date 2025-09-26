---
title: withoutAttributes
---

The `withoutAttributes` method returns a table that is identical to the source table, but with the specified attributes removed.

## Syntax

```
table.withoutAttributes(toRemove)
```

## Parameters

<ParamTable>
<Param name="toRemove>" type="Collection<String>">

The names of the attributes to remove.

</Param>
</ParamTable>

## Returns

A table with the specified attributes removed.

## Examples

The following example removes the [`AddOnly`](/core/javadoc/io/deephaven/engine/table/Table.html#ADD_ONLY_TABLE_ATTRIBUTE) attribute from a table.

```groovy order=source,result
source = timeTable("PT00:00:01")

println source.getAttributes()
println source.getAttributeKeys()

result = source.withoutAttributes(["AddOnly"])

println result.getAttributes()
```

## Related documentation

- [`getAttribute`](../metadata/getAttribute.md)
- [`getAttributeKeys`](../metadata/getAttributeKeys.md)
- [`getAttributes`](../metadata/getAttributes.md)
- [`hasAttribute`](../metadata/hasAttribute.md)
- [`retainingAttributes`](../select/retainingAttributes.md)
- [`timeTable`](./timeTable.md)
- [`withAttributes`](../select/withAttributes.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/AttributeMap.html#withoutAttributes(java.util.Collection))
