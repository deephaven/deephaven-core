---
title: getAttribute
---

The `getAttribute` method returns the value for the specified attribute key.

## Syntax

```groovy syntax
table.getAttribute(key)
```

## Parameters

<ParamTable>
<Param name="key>" type="String">

The name of the attribute.

</Param>
</ParamTable>

## Returns

The value for the specified attribute key, or `null` if there was none.

## Examples

```groovy order=:log
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
import io.deephaven.engine.table.AttributeMap

source = newTable(
    doubleCol("Doubles", 3.1, 5.45, -1.0),
    stringCol("Strings", "Creating", "New", "Tables")
)

result = AppendOnlyArrayBackedInputTable.make(source)

println source.getAttribute("AddOnly")
println result.getAttribute("AddOnly")
```

## Related documentation

- [`withoutAttributes`](../create/withoutAttributes.md)
- [`getAttributeKeys`](./getAttributeKeys.md)
- [`getAttributes`](./getAttributes.md)
- [`hasAttribute`](./hasAttribute.md)
- [`retainingAttributes`](../select/retainingAttributes.md)
- [`withAttributes`](../select/withAttributes.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/AttributeMap.html#getAttribute(java.lang.String))
