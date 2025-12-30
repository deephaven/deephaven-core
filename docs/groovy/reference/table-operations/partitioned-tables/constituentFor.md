---
title: constituentFor
---

The `constituentFor` method returns a single constituent for the supplied key column values.

> [!NOTE]
> If `constituentChangesPermitted() = true`, this method may return different results if invoked multiple times.

The result will be managed by the enclosing liveness scope.

## Syntax

```
constituentFor(keyColumnValues...)
```

## Parameters

<ParamTable>
<Param name="keyColumnValues" type="Object...">

Ordered, boxed values for the key columns, in the same order as [`keyColumnNames()`](./keyColumnNames.md).

The `keyColumnValues` can be thought of as a tuple constraining the values for the corresponding key columns for the result row.
If there are no matching rows, the result is null. If there are multiple matching rows, an `UnsupportedOperationException` is thrown.

</Param>
</ParamTable>

## Returns

A single constituent for the supplied `keyColumnValues`.

## Examples

```groovy order=:log
source = newTable(
    stringCol("X", "A", "B", "A", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "N", "O", "N", "P", "M", "O", "P", "M"),
    intCol("Number", 55, 76, 20, 130, 230, 50, 73, 137, 214),
)

partitionedTable = source.partitionBy("X")

println partitionedTable.constituentFor("Number")
```

## Related documentation

- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/PartitionedTable.html#constituentFor(java.lang.Object...))
