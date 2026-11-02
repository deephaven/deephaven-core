---
title: Data types in Deephaven
---

For performance reasons, the Deephaven engine is implemented in Java. As such, Deephaven tables use Java data types for columns, both [Java primitive types](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) and [Java objects](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html). This guide describes the primitive, object, and array types that table columns hold, then shows how to make sure a column gets the type you want with a [type cast](./casting.md).

## Java data types

### Primitive types

[Java primitive types](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) map directly to hardware representations and have fixed sizes. Deephaven stores primitive values directly rather than as objects, so they are faster and use less memory. That speed and memory saving is the main reason to use them for table columns.

The following table shows the mapping between Java primitive types and Java primitive type sizes:

| Java primitive type | Java primitive type size |
| ------------------- | ------------------------ |
| `byte`              | 1 byte                   |
| `short`             | 2 bytes                  |
| `int`               | 4 bytes                  |
| `long`              | 8 bytes                  |
| `float`             | 4 bytes                  |
| `double`            | 8 bytes                  |
| `char`              | 2 bytes                  |

Deephaven tables have no primitive `boolean` column type. A [formula](./formulas.md) that returns a `boolean` produces a [`java.lang.Boolean`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Boolean.html) column instead, and Deephaven stores each value in one byte.

A [Groovy variable](./groovy-variables.md) used in a [query string](./query-string-overview.md) produces a column of its value's type, and boxed numbers such as `Integer` and `Double` become `int` and `double` columns. Groovy decimal literals such as `3.14` are `java.math.BigDecimal`, so they produce `BigDecimal` columns unless you [cast](./casting.md#groovy-variables) them or add a `d` suffix (`3.14d`). [Groovy variables in query strings](./groovy-variables.md) shows both cases.

### Object types

Object types are Java classes whose values are objects rather than raw values. They consume more memory and are generally slower than their primitive counterparts.

Some object types are commonly used in tables despite the overhead:

- [`java.lang.String`](../reference/query-language/types/strings.md), for text.
- [`java.lang.Boolean`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Boolean.html), for true/false values.
- [`java.time.Instant`](../reference/query-language/types/date-time.md#instant), for points in time.

When Deephaven can't tell what type a [formula](./formulas.md) returns, such as a [closure](./groovy-closures.md) call, the column becomes a [`java.lang.Object`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html) column. [Data type conversions](#data-type-conversions) shows an example and how to fix it with a [type cast](./casting.md).

### Array types

Deephaven tables commonly hold two kinds of [array](./work-with-arrays.md) column:

- [Java arrays](https://docs.oracle.com/javase/specs/jls/se17/html/jls-10.html), most often of primitive types such as `int[]` or `double[]`.
- [Deephaven vectors](../reference/query-language/types/arrays.md), which table operations such as [grouping](./grouping-data.md) produce.

[Arrays vs vectors](../conceptual/arrays-vs-vectors.md) compares the two.

## Data type conversions

To make sure a column computed by a [Groovy closure](./groovy-closures.md) gets the type you want, use a [type cast](./casting.md). The following code block casts one call to the closure and leaves the other uncast, for comparison:

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

In `sourceMeta`, `Typecast` is a `double` column, while `NoTypecast` is a `java.lang.Object` column because the engine can't infer the [closure's](./groovy-closures.md) return type.

## Related documentation

- [Arrays vs vectors](../conceptual/arrays-vs-vectors.md)
- [Query string overview](./query-string-overview.md)
- [Formulas](./formulas.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Groovy closures in queries](./groovy-closures.md)
- [Groovy classes and objects in query strings](./groovy-classes.md)
- [Java objects in query strings](./java-classes.md)
- [Work with arrays](./work-with-arrays.md)
- [Group and ungroup data](./grouping-data.md)
- [Casting](./casting.md)
- [Arrays](../reference/query-language/types/arrays.md)
- [Date-time](../reference/query-language/types/date-time.md)
- [Strings](../reference/query-language/types/strings.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`meta`](../reference/table-operations/metadata/meta.md)
- [`update`](../reference/table-operations/select/update.md)
