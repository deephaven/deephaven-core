---
title: getColumnSource
---

The `getColumnSource` method returns a `ColumnSource` that provides access to a column's data by **row key**, not positional index. Several overloads can cast the result to a target type so you don't need an explicit cast when the column's data type is known.

## Syntax

```groovy syntax
table.getColumnSource(sourceName)
table.getColumnSource(sourceName, clazz)
table.getColumnSource(sourceName, clazz, componentType)
table.getColumnSource(columnDefinition)
```

## Parameters

<ParamTable>
<Param name="sourceName" type="String">

The name of the column.

</Param>
<Param name="clazz" type="Class<? extends T>">

The target data type to cast the `ColumnSource` to.

</Param>
<Param name="componentType" type="Class<?>">

The target component type, which is useful for array or vector columns. May be `null`.

</Param>
<Param name="columnDefinition" type="ColumnDefinition<? extends T>">

A `ColumnDefinition` whose data type and component type are used to cast the `ColumnSource`. The column name is taken from `columnDefinition.getName()`.

</Param>
</ParamTable>

## Returns

A `ColumnSource` for the requested column, parameterized by the target type when `clazz`, `componentType`, or a `columnDefinition` is provided.

> [!IMPORTANT]
> `ColumnSource` methods like `get(rowKey)` use **row keys**, not positional indices. Row keys are the internal identifiers for rows and may not match positional indices, especially in filtered or modified tables.

## Examples

> [!CAUTION]
> Row keys are internal identifiers, not positional indices, and cannot be assumed or made up (for example, a row key of `2` does not necessarily mean "the third row"). Always obtain row keys from the table's own [`RowSet`](https://deephaven.io/core/javadoc/io/deephaven/engine/rowset/RowSet.html) via `getRowSet()`, as shown below. For simple bulk or positional access to column data, prefer [`columnIterator`](../../../how-to-guides/extract-table-value.md) or [`ColumnVectors`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html) rather than calling single-value `ColumnSource` accessors like `getInt` directly.

The following example retrieves a `ColumnSource` by name, then uses the table's `RowSet` to visit every valid row key:

```groovy order=:log
source = newTable(
    intCol("Integers", 1, 2, 3, 4, 5)
)

columnSource = source.getColumnSource("Integers")

source.getRowSet().forAllRowKeys { rowKey ->
    println rowKey
}
```

The following example casts the `ColumnSource` to a target type using a `Class` and using a `ColumnDefinition` from the table's definition:

```groovy order=:log
source = newTable(
    intCol("Integers", 1, 2, 3, 4, 5)
)

// Cast to a target type by class
intSource = source.getColumnSource("Integers", int.class)
println intSource

// Cast using a ColumnDefinition from the table's definition
intDef = source.getDefinition().getColumn("Integers")
defSource = source.getColumnSource(intDef)
println defSource
```

## Related documentation

- [Extract table values](../../../how-to-guides/extract-table-value.md)
- [getDefinition](./getDefinition.md)
- [`ColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html)
- [`ColumnDefinition`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnDefinition.html)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#getColumnSource(java.lang.String))
- [Javadoc (ColumnDefinition overload)](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#getColumnSource(io.deephaven.engine.table.ColumnDefinition))
