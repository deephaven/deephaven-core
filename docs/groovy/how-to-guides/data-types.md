---
title: Data types in Deephaven and Python
sidebar_label: Data types
---

For performance reasons, the Deephaven engine is implemented in Java. As such, Deephaven tables use Java data types for columns. These include both [Java primitive types](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) and [Java objects](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html).

## Java data types

### Primitive types

[Java primitive types](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) are tuned to physical hardware and are fixed sizes. This is the main reason they are preferred in tables - they are faster and more memory efficient.

The following table shows the mapping between Java primitive types and Java primitive type sizes:

| Java primitive type | Java primitive type size |
| ------------------- | ------------------------ |
| `boolean`           | 1 byte                   |
| `byte`              | 1 byte                   |
| `short`             | 2 bytes                  |
| `int`               | 4 bytes                  |
| `long`              | 8 bytes                  |
| `float`             | 4 bytes                  |
| `double`            | 8 bytes                  |
| `char`              | 2 bytes                  |

### Array types

In Deephaven tables, there are two commonly used [array](./work-with-arrays.md) types:

- [Deephaven Vectors](../reference/query-language/types/arrays.md)
- [Java arrays](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Arrays.html)

## Data type conversions

You can ensure that columns in tables are of the appropriate type by using [type casting](./casting.md):

```groovy order=source,sourceMeta
func = { value ->
    return Math.sqrt(value)
}

source = emptyTable(10).update(
    "Typecast = (double)func(ii)",
    "NoTypecast = func(ii)"
)
sourceMeta = source.meta()
```

## Related documentation

- [Query string overview](./query-string-overview.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Groovy functions in query strings](./groovy-closures.md)
- [Groovy classes and objects in query strings](./groovy-classes.md)
- [Java objects in query strings](./java-classes.md)
- [Work with arrays](./work-with-arrays.md)
