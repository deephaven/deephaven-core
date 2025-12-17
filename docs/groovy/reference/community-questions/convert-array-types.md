---
title: How do I convert a vector column to a primitive array column?
sidebar_label: How do I convert vector columns to primitive array columns?
---

_How can I convert a Deephaven vector column to a primitive array column in Deephaven?_

You can convert a Deephaven vector column to a primitive array column by using the `array()` method in a selection method's query string. Similarly, the `vec()` method can convert a primitive array type to a Deephaven vector type. Here, the `array` method converts an `IntVector` column to an `int[]` array. Here's an example:

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

The `vec` and `array` methods can handle the following types:

| `vec()`  | `array()`    |
| -------- | ------------ |
| byte[]   | ByteVector   |
| char[]   | CharVector   |
| double[] | DoubleVector |
| float[]  | FloatVector  |
| int[]    | IntVector    |
| long[]   | LongVector   |
| short[]  | ShortVector  |

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
