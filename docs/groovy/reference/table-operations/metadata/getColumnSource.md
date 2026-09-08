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

A `ColumnSource` is most useful in code that works with row keys directly, such as a [table listener](../../../how-to-guides/table-listeners-groovy.md). A listener's `onUpdate` method receives a [`TableUpdate`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/TableUpdate.html) that describes changed rows by row key, and a `ColumnSource` is how you resolve those row keys to values.

> [!CAUTION]
> Row keys are internal identifiers, not positional indices, and cannot be assumed or made up (for example, a row key of `2` does not necessarily mean "the third row"). Always obtain row keys from a [`TableUpdate`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/TableUpdate.html) or the table's own [`RowSet`](https://deephaven.io/core/javadoc/io/deephaven/engine/rowset/RowSet.html) via `getRowSet()`. For simple bulk or positional access to column data, prefer [`columnIterator`](../../../how-to-guides/extract-table-value.md) or [`ColumnVectors`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html) rather than calling single-value `ColumnSource` accessors like `getInt` directly.

The following example listens for rows added to a ticking table. Each update cycle, the listener reads the new rows' values through two column sources: one retrieved with the `clazz` overload and one with the `columnDefinition` overload. Both overloads return a typed `ColumnSource`, so reads don't require a cast:

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.ColumnSource
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter
import io.deephaven.engine.rowset.RowSet
import java.time.Instant

t1 = timeTable("PT1S").update("X = 100 + i").tail(5)

h1 = new InstrumentedTableUpdateListenerAdapter(t1, false) {
    @Override
    public void onUpdate(TableUpdate upstream) {
        // Cast to a target type by class
        ColumnSource<Instant> tsSource = t1.getColumnSource("Timestamp", Instant.class)

        // Cast using a ColumnDefinition from the table's definition
        xDef = t1.getDefinition().getColumn("X")
        ColumnSource<Integer> xSource = t1.getColumnSource(xDef)

        // The TableUpdate provides the row keys of the added rows
        RowSet.Iterator iter = upstream.added().iterator()
        while (iter.hasNext()) {
            long rowKey = iter.next()
            Instant ts = DateTimeUtils.epochNanosToInstant(tsSource.getLong(rowKey))
            int x = xSource.getInt(rowKey)
            println "Added row: Timestamp=${ts}, X=${x}"
        }
    }
}

t1.addUpdateListener(h1)
```

## Related documentation

- [Extract table values](../../../how-to-guides/extract-table-value.md)
- [Table listeners](../../../how-to-guides/table-listeners-groovy.md)
- [getDefinition](./getDefinition.md)
- [`ColumnSource`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnSource.html)
- [`ColumnDefinition`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/ColumnDefinition.html)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#getColumnSource(java.lang.String))
- [Javadoc (ColumnDefinition overload)](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#getColumnSource(io.deephaven.engine.table.ColumnDefinition))
