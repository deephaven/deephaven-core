---
title: Data types in Deephaven and Python
sidebar_label: Data types
---

For performance reasons, the Deephaven engine is implemented in Java. As such, Deephaven tables use Java data types for columns. These include both [Java primitive types](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) and [Java objects](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html).

Deephaven Python queries combine the power of Python with Deephaven's tables. This mixing of Python and Java has some important ramifications for data types. A conceptual understanding of how Python and Java types relate is critical to writing effective queries.

## Python data types

In Python, everything is an object. That includes even scalar types like `int` and `float`. This means that all Python objects have additional properties and methods. This is different from Java, which has [primitive data types](#primitive-types) as well as objects (also known as boxed types). The mapping of Python data types to Java data types is given in the [Java data types](#java-data-types) section.

## Java data types

### Primitive types

[Java primitive types](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) are tuned to physical hardware and are fixed sizes, unlike Python data types. This is the main reason they are preferred in tables - they are faster and more memory efficient.

The following table shows the mapping between Java primitive types, Java primitive type sizes, Python types, and [NumPy types](https://numpy.org/devdocs/user/basics.types.html):

| Java primitive type | Java primitive type size | Python type | NumPy type  |
| ------------------- | ------------------------ | ----------- | ----------- |
| `boolean`           | 1 byte                   | `bool`      | `np.bool_`  |
| `byte`              | 1 byte                   | N/A         | `np.byte`   |
| `short`             | 2 bytes                  | N/A         | `np.short`  |
| `int`               | 4 bytes                  | N/A         | `np.intc`   |
| `long`              | 8 bytes                  | `int`       | `np.int_`   |
| `float`             | 4 bytes                  | N/A         | `np.single` |
| `double`            | 8 bytes                  | `float`     | `np.double` |
| `char`              | 2 bytes                  | N/A         | N/A         |

### Object types

Object types are data types that cannot be represented as primitive data types. This means they consume more memory and are generally less performant than their primitive counterparts. The concept is similar in both Java and Python.

Some object types are commonly used despite the overhead. For instance, [`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html) is the best way to store text in tables.

The following table shows a mapping between some of the most commonly used Java object types and their Python and [NumPy](https://numpy.org/) equivalents:

| Java object type                                                                                           | Python type                                                                              | NumPy type                                                                                      |
| ---------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| [`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html)   | [`str`](https://docs.python.org/3/library/stdtypes.html#text-sequence-type-str)          | [`np.str_`](https://numpy.org/devdocs/reference/arrays.scalars.html#numpy.str_)                 |
| [`java.time.Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) | [`datetime.datetime`](https://docs.python.org/3/library/datetime.html#datetime.datetime) | [`np.datetime64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.datetime64) |
| [Array](https://docs.oracle.com/javase/specs/jls/se7/html/jls-10.html)                                     | [`Sequence`](https://docs.python.org/3/glossary.html#term-sequence)                      | [`np.ndarray`](https://numpy.org/devdocs/reference/arrays.html)                                 |
| [`java.lang.Object`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html)   | [`Object`](https://docs.python.org/3/glossary.html#term-object)                          | [`np.object_`](https://numpy.org/doc/stable/reference/arrays.dtypes.html)                       |

### Array types

In Python, [sequences](https://docs.python.org/3/glossary.html#term-sequence) are the overarching term used to define iterable, ordered collections of data. In Deephaven tables, there are two commonly used [array](./work-with-arrays.md) types:

- [Deephaven Vectors](../reference/query-language/types/arrays.md)
- [Java arrays](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Arrays.html)

### PyObjects

[`org.jpy.PyObject`](./pyobjects.md) is a Java wrapper around arbitrary Python objects used by Deephaven's [jpy](./use-jpy.md) bridge. They typically appear when [Python functions](./python-functions.md) without [type hints](./python-functions.md#type-hints) or [type casts](./casting.md) are called in query strings.

While flexible enough to represent any Python object (including those without Java equivalents, like tuples or dictionaries), PyObjects have significant limitations:

- Limited compatibility with Deephaven's [built-in functions](./built-in-functions.md)
- Reduced performance compared to Java primitive types
- Higher memory usage due to their boxed nature

Use [PyObjects](./pyobjects.md) only when working with complex Python objects that have no Java equivalent. Otherwise, prefer [type hints](./python-functions.md#type-hints) or explicit [type casts](./casting.md) to ensure better performance and type safety.

## Data type conversions

There are two ways to ensure that columns in tables are of the appropriate type: [type hints](./python-functions.md#type-hints) and [type casts](./casting.md). The following code block shows a simple example of both:

```python order=source,source_meta
from deephaven import empty_table


def func_without_type_hint(value):
    return value**0.5


def func_with_type_hint(value: int) -> float:
    return value**0.5


source = empty_table(10).update(
    [
        "Typecast = (double)func_without_type_hint(ii)",
        "TypeHint = func_with_type_hint(ii)",
    ]
)
source_meta = source.meta_table
```

## Related documentation

- [Query string overview](./query-string-overview.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [Python classes and objects in query strings](./python-classes.md)
- [Java objects in query strings](./java-classes.md)
- [Work with arrays](./work-with-arrays.md)
