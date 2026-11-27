---
title: How do I convert a vector column to a primitive array column?
---

_How can I convert a Deephaven vector column to a primitive array column?_

You can convert a [Deephaven vector](../../conceptual/arrays-vs-vectors.md) column to a primitive array column by using the [`array`](../query-language/query-library/auto-imported/basic.md) method in an [`update`](../table-operations/select/update.md) query string. Similarly, the [`vec`](../query-language/query-library/auto-imported/basic.md) method converts a primitive array type back to a Deephaven vector type. The following example groups a column into an [`IntVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/IntVector.html), converts it to an `int[]` with `array`, and converts it back with `vec`:

```groovy order=t,tm,t1,t1m,t2,t2m
// Create a table with an IntVector column
t = emptyTable(10).update("X = i%2", "Y = i").groupBy("X")
tm = t.meta()

// Convert the IntVector column to an int[] column with array()
t1 = t.update("YJArray = array(Y)")
t1m = t1.meta()

// Convert the int[] column to an IntVector column with vec()
t2 = t1.update("YDHVec = vec(YJArray)")
t2m = t2.meta()
```

`vec` converts each primitive array type to the corresponding Deephaven vector type, and `array` converts the other way:

| Primitive array | Deephaven vector |
| --------------- | ---------------- |
| `byte[]`        | `ByteVector`     |
| `char[]`        | `CharVector`     |
| `double[]`      | `DoubleVector`   |
| `float[]`       | `FloatVector`    |
| `int[]`         | `IntVector`      |
| `long[]`        | `LongVector`     |
| `short[]`       | `ShortVector`    |

For object columns such as `String`, use `arrayObj` to convert an `ObjectVector` to a Java array and `vecObj` to convert back.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
