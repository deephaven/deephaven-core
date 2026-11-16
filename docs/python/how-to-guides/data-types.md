---
title: Data types in Deephaven and Python
---

For performance reasons, the Deephaven engine is implemented in Java. As such, Deephaven tables use Java data types for columns, both [Java primitive types](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) and [Java objects](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html).

Deephaven Python queries combine the power of Python with Deephaven's tables. Table columns hold Java types, so every value a query puts in a column has a Java type. Knowing how Python and Java types map to each other helps you write queries that are fast and produce the column types you expect.

The [Java data types](#java-data-types) section maps Python and NumPy types to the Java types that columns hold. [Data type conversions](#data-type-conversions) then shows how to make sure a column gets the type you want with [type hints](./python-functions.md#type-hints) or [type casts](./casting.md).

## Java data types

### Primitive types

In Python, everything is an object, including scalar types like `int` and `float`. A Python `int` carries methods and attributes that a Java primitive value does not. Java, by contrast, has primitive types as well as objects.

[Java primitive types](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) map directly to hardware representations and have fixed sizes, unlike Python's arbitrary-precision `int`. Deephaven stores primitive values directly rather than as objects, so they are faster and use less memory. That speed and memory saving is the main reason to use them for table columns.

The following table shows the mapping between Java primitive types, Java primitive type sizes, and the Python and [NumPy types](https://numpy.org/devdocs/user/basics.types.html) that map to them in a function's [type hints](./python-functions.md#type-hints):

| Java primitive type | Java primitive type size | Python type | NumPy type   |
| ------------------- | ------------------------ | ----------- | ------------ |
| `byte`              | 1 byte                   | N/A         | `np.int8`    |
| `short`             | 2 bytes                  | N/A         | `np.int16`   |
| `int`               | 4 bytes                  | N/A         | `np.int32`   |
| `long`              | 8 bytes                  | `int`       | `np.int64`   |
| `float`             | 4 bytes                  | N/A         | `np.float32` |
| `double`            | 8 bytes                  | `float`     | `np.float64` |
| `char`              | 2 bytes                  | N/A         | `np.uint16`  |

Deephaven tables have no primitive `boolean` column type. True/false values go in [`java.lang.Boolean`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Boolean.html) columns, which Deephaven stores as one byte per value:

- A [formula](./formulas.md) that returns a Java `boolean` produces a `java.lang.Boolean` column.
- In a function's [type hints](./python-functions.md#type-hints), Python `bool` and `np.bool_` map to `java.lang.Boolean`, as the [object types](#object-types) table shows.

[Variables](./python-variables.md) used directly in a [query string](./query-string-overview.md) follow different rules from [type hints](./python-functions.md#type-hints). For example, a Python `int` variable becomes the smallest Java integer type that holds its value. Most NumPy scalar variables become [PyObjects](#pyobjects). [Python variables in query strings](./python-variables.md) explains how each kind of Python variable converts.

### Object types

Object types are Java classes whose values are objects rather than raw values. They consume more memory and are generally slower than their primitive counterparts.

Some object types are commonly used despite the overhead. For instance, [`java.lang.String`](../reference/query-language/types/strings.md) is the best way to store text in tables.

The following table shows the Python and [NumPy](https://numpy.org/) types that map to common Java object types in a function's [type hints](./python-functions.md#type-hints). The last row, [`org.jpy.PyObject`](./pyobjects.md), is the Java wrapper for a Python value that has no Java mapping:

| Java object type                                                                                           | Python type                                                                                     | NumPy type                                                                                                            |
| ---------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| [`java.lang.String`](../reference/query-language/types/strings.md)                                         | [`str`](https://docs.python.org/3/library/stdtypes.html#text-sequence-type-str)                 | [`np.str_`](https://numpy.org/devdocs/reference/arrays.scalars.html#numpy.str_)                                       |
| [`java.lang.Boolean`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Boolean.html) | [`bool`](https://docs.python.org/3/library/stdtypes.html#boolean-type-bool)                     | [`np.bool_`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.bool)                                   |
| [`java.time.Instant`](../reference/query-language/types/date-time.md#instant)                              | [`datetime.datetime`](https://docs.python.org/3/library/datetime.html#datetime.datetime)        | [`np.datetime64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.datetime64)                        |
| [Array](https://docs.oracle.com/javase/specs/jls/se17/html/jls-10.html)                                    | [`Sequence[T]`](https://docs.python.org/3/glossary.html#term-sequence), such as `Sequence[int]` | [`np.ndarray`](https://numpy.org/devdocs/reference/arrays.html) with an element type, such as `npt.NDArray[np.int64]` |
| [`org.jpy.PyObject`](./pyobjects.md)                                                                       | [`object`](https://docs.python.org/3/glossary.html#term-object)                                 | [`np.object_`](https://numpy.org/doc/stable/reference/arrays.dtypes.html)                                             |

An array [type hint's](./python-functions.md#type-hints) element type follows the same mapping, so `Sequence[int]` and `npt.NDArray[np.int64]` both produce a `long[]` column.

For every type Deephaven defines in Python, see [`deephaven.dtypes`](../reference/python/deephaven-python-types.md).

### Array types

Deephaven tables commonly hold two kinds of [array](./work-with-arrays.md) column:

- [Java arrays](https://docs.oracle.com/javase/specs/jls/se17/html/jls-10.html), most often of primitive types such as `int[]` or `double[]`.
- [Deephaven vectors](../reference/query-language/types/arrays.md), which table operations such as [grouping](./grouping-data.md) produce.

[Arrays vs vectors](../conceptual/arrays-vs-vectors.md) compares the two.

A Python list used in a [query string](./query-string-overview.md) without type information becomes an [`org.jpy.PyListWrapper`](./work-with-arrays.md#pylistwrapper-columns) column, not a Java array. Tuples, NumPy arrays, and other sequences that are not lists or strings become [PyObjects](./pyobjects.md).

To get a Java array column, convert the sequence first with [`jpy.array`](/core/pydoc/code/jpy.html#jpy.array) or [`deephaven.dtypes.array`](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.array). [Work with arrays](./work-with-arrays.md#pylistwrapper-columns) shows how.

### PyObjects

[`org.jpy.PyObject`](./pyobjects.md) is the Java wrapper that the [jpy](./use-jpy.md) Java-Python bridge puts around arbitrary Python objects.

A [PyObject](./pyobjects.md) column typically appears when a [query string](./query-string-overview.md) uses one of these Python values:

- Most NumPy scalar variables, such as `np.int64`, `np.float32`, or `np.bool_`. The exceptions are `np.float64` and `np.str_`, which become `double` and [`java.lang.String`](../reference/query-language/types/strings.md) values.
- A tuple or NumPy array.
- A class or instance attribute, such as `obj.x`. See [Python classes and objects in query strings](./python-classes.md).
- The result of a [Python function](./python-functions.md) whose return type has no Java mapping. This happens when the function has no return [type hint](./python-functions.md#type-hints), is annotated `-> object`, or uses a hint such as `-> dict`. A [type cast](./casting.md) on the call avoids this.

In these cases, the column holds [PyObjects](./pyobjects.md), not plain `java.lang.Object` values.

[PyObjects](./pyobjects.md) can represent any Python object, such as a dictionary or a custom class instance returned from a function that has no [type hint](./python-functions.md#type-hints). That flexibility comes with significant limitations:

- Limited compatibility with Deephaven's [built-in functions](./built-in-functions.md)
- Reduced performance compared to Java primitive types
- Higher memory usage, because each value is a separate object rather than a primitive

Use [PyObjects](./pyobjects.md) only when working with complex Python objects that have no Java equivalent. Otherwise, use [type hints](./python-functions.md#type-hints) or explicit [type casts](./casting.md) so the column gets a Java type.

## Data type conversions

There are two ways to make sure a column computed by a [Python function](./python-functions.md) has the type you want: [type hints](./python-functions.md#type-hints) and [type casts](./casting.md). The following code block uses both, plus a column that uses neither, for comparison:

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
        "NoTypeHint = func_without_type_hint(ii)",
    ]
)
source_meta = source.meta_table
```

In `source_meta`, `Typecast` and `TypeHint` are `double` columns, while `NoTypeHint` is an [`org.jpy.PyObject`](./pyobjects.md) column because its function has no [type hint](./python-functions.md#type-hints) and its [formula](./formulas.md) has no [type cast](./casting.md).

## Related documentation

- [Arrays vs vectors](../conceptual/arrays-vs-vectors.md)
- [Query string overview](./query-string-overview.md)
- [Formulas](./formulas.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [Python classes and objects in query strings](./python-classes.md)
- [Java objects in query strings](./java-classes.md)
- [Work with arrays](./work-with-arrays.md)
- [Group and ungroup data](./grouping-data.md)
- [Casting](./casting.md)
- [Handle PyObjects in tables](./pyobjects.md)
- [Use jpy](./use-jpy.md)
- [Built-in functions](./built-in-functions.md)
- [Deephaven Python data types](../reference/python/deephaven-python-types.md)
- [Arrays](../reference/query-language/types/arrays.md)
- [Date-time](../reference/query-language/types/date-time.md)
- [Strings](../reference/query-language/types/strings.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`meta_table`](../reference/table-operations/metadata/meta_table.md)
- [`update`](../reference/table-operations/select/update.md)
- [`deephaven.dtypes` Pydoc](/core/pydoc/code/deephaven.dtypes.html)
