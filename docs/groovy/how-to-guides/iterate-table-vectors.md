---
title: Iterate over tables with ColumnVectors
sidebar_label: Iterate over tables
---

This guide will show you how to iterate over table data in Groovy queries via [`ColumnVectors`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html). [`ColumnVectors`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html) is a helper class that makes it easy to iterate over a column of data. When iterating over columns via these vectors, the iterator uses chunks internally for high performance and lower overhead.

## Column Vectors

Deephaven's [`ColumnVectors`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html) provide utility methods for constructing [`Vectors`](https://docs.deephaven.io/core/javadoc/io/deephaven/vector/Vector.html) from columns in tables. These provide bulk and random access to column data by row position.

> [!NOTE]
> It's recommended to avoid random access when possible, as it's less efficient than bulk access.

For refreshing tables, a vector is only valid for the current update cycle, or the updating phase if `previousValues=true`. See [engine locking](../conceptual/query-engine/engine-locking.md) for more information on concurrent and consistent data access.

Column vectors are created from specific data types. For example, for an `int` column, use `ColumnVectors.ofInt`; for a `double` column, use `ColumnVectors.ofDouble`. For generic or non-primitive types, use `ColumnVectors.ofObject`.

Column vectors require the following import statement:

```groovy test-set=1 order=null
import io.deephaven.engine.table.vectors.ColumnVectors
```

## Iterate over table data

The examples in this guide use the following table:

```groovy test-set=1 order=source
source = emptyTable(100).updateView("X = randomInt(0, 100)", "Y = randomDouble(0, 1)", "Z = (i % 2 == 0) ? `Even` : `Odd`")
```

To iterate over a specific column, create a vector of the given type, and then construct an `iterator` from the vector. The following example iterates over the `X` column:

```groovy test-set=1 order=null
intVec = ColumnVectors.ofInt(source, 'X')

intIterator = intVec.iterator()

while (intIterator.hasNext()) {
    int value = intIterator.next()
    // Do something with the value
}
```

The following example iterates over the `Y` column:

```groovy test-set=1 order=null
doubleVec = ColumnVectors.ofDouble(source, 'Y')

doubleIterator = doubleVec.iterator()

while (doubleIterator.hasNext()) {
    double value = doubleIterator.next()
    // Do something with the value
}
```

The following example iterates over the `Z` column. Note how the class must be passed into the vector constructor so that the engine knows what data type it's working with:

```groovy test-set=1 order=null
objectVec = ColumnVectors.ofObject(source, 'Z', java.lang.String)

objectIterator = objectVec.iterator()

while (objectIterator.hasNext()) {
    String value = objectIterator.next()
    // Do something with the value
}
```

## Related documentation

- [Engine locking](../conceptual/query-engine/engine-locking.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html)
