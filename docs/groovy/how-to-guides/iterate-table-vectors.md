---
title: Iterate over tables with ColumnVectors
---

This guide shows you how to iterate over table data in Groovy queries using [`ColumnVectors`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html).

> [!NOTE]
> Iterate over a column when Groovy code must handle each value, for example to log values or pass them to other code. To compute a result such as a sum or a maximum, use a table operation such as [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md) instead. The examples below compute simple results only to keep them short. See [Recipes, not loops!](../getting-started/crash-course/vectorization-vs-loops.md).

## The `ColumnVectors` class

[`ColumnVectors`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html) is a helper class with factory methods that create a [`Vector`](https://deephaven.io/core/javadoc/io/deephaven/vector/Vector.html) from a table column. A vector provides bulk and random access to column data by row position.

Iterating over a vector is bulk access. The iterator walks the rows in order instead of looking up each row position separately, and for larger columns it reads values in blocks. This is faster than reading each value by position.

`ColumnVectors` has a typed factory method for each primitive column type, an [`ofObject`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html#ofObject(io.deephaven.engine.table.Table,java.lang.String,java.lang.Class)) method for all other column types, and a generic [`of`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html#of(io.deephaven.engine.table.Table,java.lang.String)) method that is declared to return [`Vector<?>`](https://deephaven.io/core/javadoc/io/deephaven/vector/Vector.html) rather than a specific vector type. Prefer the typed method that matches the column's type:

- For a `char`, `byte`, `short`, `int`, `long`, `float`, or `double` column, use the matching primitive method: [`ofChar`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html#ofChar(io.deephaven.engine.table.Table,java.lang.String)), [`ofByte`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html#ofByte(io.deephaven.engine.table.Table,java.lang.String)), [`ofShort`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html#ofShort(io.deephaven.engine.table.Table,java.lang.String)), [`ofInt`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html#ofInt(io.deephaven.engine.table.Table,java.lang.String)), [`ofLong`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html#ofLong(io.deephaven.engine.table.Table,java.lang.String)), [`ofFloat`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html#ofFloat(io.deephaven.engine.table.Table,java.lang.String)), or [`ofDouble`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html#ofDouble(io.deephaven.engine.table.Table,java.lang.String)).
- For any other type, including `String` and `Boolean`, use [`ofObject`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html#ofObject(io.deephaven.engine.table.Table,java.lang.String,java.lang.Class)) and pass the column's class as the third argument. Deephaven checks that the column's data type can be cast to that class and throws a `ClassCastException` if it can't.

> [!NOTE]
> To read individual values by row position instead, see [Extract table values](./extract-table-value.md). A vector's iterator reads values the same way as a table's column iterator, so create a vector when you also need its other methods, such as `size`, `get`, or `copyToArray`, or want to pass a `Vector` to other code. To iterate over a column without creating a vector, call the table's [`columnIterator`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#columnIterator(java.lang.String)) method or a typed variant such as [`integerColumnIterator`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#integerColumnIterator(java.lang.String)). See [Iterate over a column](./extract-table-value.md#iterate-over-a-column).

## Iterate over a column vector

The examples in this guide import [`ColumnVectors`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html) and use the following table:

```groovy test-set=1 order=source
import io.deephaven.engine.table.vectors.ColumnVectors

source = emptyTable(1000).update("X = i % 100", "Y = (i % 97) / 97.0", "Z = (i % 2 == 0) ? `Even` : `Odd`")
```

To iterate over a specific column, create a vector that matches the column's type and then call the vector's `iterator` method, such as [`IntVector.iterator`](https://deephaven.io/core/javadoc/io/deephaven/vector/IntVector.html#iterator()). Read each value with the iterator's matching `next` method, such as [`nextInt`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/PrimitiveIterator.OfInt.html#nextInt()) for an [`IntVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/IntVector.html), [`nextDouble`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/PrimitiveIterator.OfDouble.html#nextDouble()) for a [`DoubleVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/DoubleVector.html), or [`next`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Iterator.html#next()) for an [`ObjectVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/ObjectVector.html).

The following example iterates over the `X` column and sums its values:

```groovy test-set=1 order=:log
intVec = ColumnVectors.ofInt(source, 'X')

intIterator = intVec.iterator()

long total = 0
while (intIterator.hasNext()) {
    int value = intIterator.nextInt()
    total += value
}
println "Sum of X: ${total}"
```

The following example iterates over the `Y` column and finds its largest value:

```groovy test-set=1 order=:log
doubleVec = ColumnVectors.ofDouble(source, 'Y')

doubleIterator = doubleVec.iterator()

double maxY = Double.NEGATIVE_INFINITY
while (doubleIterator.hasNext()) {
    double value = doubleIterator.nextDouble()
    maxY = Math.max(maxY, value)
}
println "Largest Y: ${maxY}"
```

The following example iterates over the `Z` column, a `String` column read with [`ofObject`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html#ofObject(io.deephaven.engine.table.Table,java.lang.String,java.lang.Class)), and counts the `Even` values:

```groovy test-set=1 order=:log
objectVec = ColumnVectors.ofObject(source, 'Z', String)

objectIterator = objectVec.iterator()

int evenCount = 0
while (objectIterator.hasNext()) {
    String value = objectIterator.next()
    if (value == 'Even') {
        evenCount++
    }
}
println "Even values in Z: ${evenCount}"
```

Each loop above runs until [`hasNext`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Iterator.html#hasNext()) returns `false`, at which point the iterator releases its resources. If you stop iterating early, for example with `break`, call the iterator's [`close`](https://deephaven.io/core/javadoc/io/deephaven/engine/primitive/iterator/CloseableIterator.html#close()) method. The following example stops at the first `X` value over 90 and closes the iterator in a `finally` block:

```groovy test-set=1 order=:log
firstLarge = null
intIterator = ColumnVectors.ofInt(source, 'X').iterator()
try {
    while (intIterator.hasNext()) {
        int value = intIterator.nextInt()
        if (value > 90) {
            firstLarge = value
            break
        }
    }
} finally {
    intIterator.close()
}
println "First X over 90: ${firstLarge}"
```

## Refreshing tables

For a [refreshing table](../conceptual/table-types.md), a vector reflects the table's data only until the engine starts its next [update cycle](../conceptual/table-update-model.md). To use it safely:

- Create the vector and iterate over it within the same cycle. To do this, hold the UpdateGraph's shared lock while you create and iterate over the vector, as described in [Engine locking](../conceptual/query-engine/engine-locking.md).
- To keep the data beyond the current cycle, copy it with [`copyToArray`](https://deephaven.io/core/javadoc/io/deephaven/vector/Vector.html#copyToArray()) or [`getDirect`](https://deephaven.io/core/javadoc/io/deephaven/vector/Vector.html#getDirect()) before the cycle ends.

Commands in the Deephaven console and [Application Mode](./application-mode.md) startup scripts don't need the shared lock, because they already run under the UpdateGraph's exclusive lock. A [table listener](./table-listeners-groovy.md) doesn't need it either, because the engine runs listeners while it is safe to read the table's data.

Inside a [table listener](./table-listeners-groovy.md), you can read the values the table had before the current update. Pass `true` as the final `usePreviousValues` argument of a [`ColumnVectors`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html) factory method. For example, `ColumnVectors.ofInt(myTable, 'X', true)` reads the previous values of column `X`, where `myTable` is the refreshing table the listener watches. A previous-values vector has these limits:

- It is valid only while the engine is processing an update, for example during the listener's `onUpdate` call.
- Its contents are undefined in the cycle in which `myTable` is created.

The following listener prints the previous and current values of `X` each time `myTable` updates:

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter
import io.deephaven.engine.table.vectors.ColumnVectors

myTable = timeTable("PT1S").update("X = ii").tail(3)

listener = new InstrumentedTableUpdateListenerAdapter("listener", myTable, false) {
    @Override
    void onUpdate(TableUpdate upstream) {
        def current = ColumnVectors.ofLong(myTable, 'X')
        def previous = ColumnVectors.ofLong(myTable, 'X', true)
        println "Previous X: ${previous}, current X: ${current}"
    }
}

myTable.addUpdateListener(listener)
```

## Related documentation

- [Arrays vs. vectors](../conceptual/arrays-vs-vectors.md)
- [Engine locking](../conceptual/query-engine/engine-locking.md)
- [Extract table values](./extract-table-value.md)
- [ColumnVectors Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html)
