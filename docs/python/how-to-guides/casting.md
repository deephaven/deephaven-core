---
title: Casting
---

Data type conversion, or casting for short, is the process of changing a value from one data type to another. This guide shows how to use type casts in [query strings](./query-string-overview.md) to control column types. It covers common numeric and string conversions, then casts on values from Python variables, functions, and classes.

In tables, casting gives a column the data type that later operations need. Casting to a Java primitive type also keeps values in compact primitive columns instead of [PyObject](./pyobjects.md) columns, which use more memory and are slower to work with.

For more information on why casting is important in Python queries, see:

- [The Python-Java boundary](../conceptual/python-java-boundary.md)
- [PyObjects in tables](./pyobjects.md)

## Type casts

A type cast is an expression that converts a value from one data type to another. In Python, you convert types by calling the target type:

```python order=null
a = 3
b = float(a)
```

In Deephaven, type casts in [query strings](./query-string-overview.md) convert a value or column to another data type. They use [Java syntax](https://www.w3schools.com/java/java_type_casting.asp) and follow these rules:

- **Between primitives**: a cast between two numeric [primitive types](./data-types.md#primitive-types) widens or narrows the value:
  - Widening, such as `(long)` on an `int`, keeps the value.
  - Narrowing, such as `(int)` on a `double`, can lose information. `(int)` drops a `double`'s fractional part.
  - A value outside the target type's range wraps or clamps. A `double` outside the `float` range becomes infinity.
  - A result that equals the target type's [null constant](../reference/query-language/types/nulls.md) reads as null. For `byte`, that constant is `-128`.
- **Boxed to primitive**: a boxed type is a Java object that wraps a primitive value, such as `java.lang.Double`. A cast from a boxed type to a primitive type works only for the matching primitive type or a wider one.
- **Other objects to primitive**: a cast from any other object type, such as `java.lang.Object`, to a numeric primitive type other than `char` converts the value with the matching `java.lang.Number` method, such as `intValue`. It works whenever the value is a number, even where a plain Java cast would fail. A cast from a [`PyObject`](./pyobjects.md) follows the **From a PyObject** rule instead.
- **To an object type**: a cast to an object type, such as `(String)`, is a plain Java cast. It sets the column type. It works only when the value is already an instance of that type, or when it boxes a primitive into its own boxed type or a supertype, such as `(Integer)` or `(Object)` on an `int`. `(String)` is short for `(java.lang.String)`.
- **From a PyObject**: a cast from a [`PyObject`](./pyobjects.md) to any primitive type, or to `String` or `Boolean`, converts the value through Python. Python `None` becomes the target type's null value, except for `boolean`, where it raises an error.
- **Null safety**: casts to primitive types other than `boolean` are null-safe. A null value becomes the [null constant](../reference/query-language/types/nulls.md) of the target type.

For more casting examples, see the [casting reference](../reference/query-language/control-flow/casting.md).

## Common casting scenarios

### Numeric conversions

The following query applies a numeric cast to each column:

- `ByteVal`: `(byte)128` overflows to `-128`. That value is the `byte` [null constant](../reference/query-language/types/nulls.md), so `ByteVal` reads as null.
- `ShortVal`: `(short)32767` is the largest `short` value, so it fits and is unchanged.
- `IntVal`: `(int)3.99` truncates to `3`.
- `LongVal` and `DoubleVal`: `(long)` and `(double)` widen the `int` column `IntVal`.
- `FloatVal`: `(float)` rounds a `double` literal to `float` precision.

The `source_meta` table, from [`meta_table`](../reference/table-operations/metadata/meta_table.md), lists each column's resulting type:

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(1).update(
    [
        "ByteVal = (byte)128",
        "ShortVal = (short)32767",
        "IntVal = (int)3.99",
        "LongVal = (long)IntVal",
        "FloatVal = (float)3.14159265359",
        "DoubleVal = (double)IntVal",
    ]
)
source_meta = source.meta_table
```

### String conversions

A Java cast can't convert between [strings](./work-with-strings.md) and numbers, so call a conversion function instead. [`parseInt`](../reference/query-language/query-library/auto-imported/parse.md) parses a string as an `int`, and `Integer.toString` formats an `int` as a string:

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(1).update(
    [
        "StringNum = `123`",
        "IntFromString = parseInt(StringNum)",
        "IntToString = Integer.toString(456)",
    ]
)
source_meta = source.meta_table
```

## Casts with Python objects

How you cast a value from Python depends on where it comes from: a [Python variable](./python-variables.md), a [Python function](./python-functions.md), or a method of a [Python class](./python-classes.md). The following sections cover each.

### Python variables

Python `int` and `float` [variables](./python-variables.md) reach the [query string](./query-string-overview.md) as boxed Java objects, such as `java.lang.Byte` or `java.lang.Double`. The **Boxed to primitive** rule under [Type casts](#type-casts) decides which casts work on them.

NumPy integer scalars, such as `numpy.int64`, arrive as [PyObjects](./pyobjects.md) instead. A numeric cast on one converts the value through Python, as the **From a PyObject** rule under [Type casts](#type-casts) describes.

The following query creates two columns from a [Python variable](./python-variables.md). A Python `int` becomes the smallest Java integer type that holds its value, so `a = 1` arrives as a `java.lang.Byte`:

- `ByteA` uses the variable directly, so it is a `byte` column.
- `IntA` casts the variable to a [Java primitive](./data-types.md#primitive-types) `int`. The cast works because `int` is wider than `byte`.

```python order=source,source_meta
from deephaven import empty_table

a = 1

source = empty_table(1).update(
    [
        "ByteA = a",
        "IntA = (int)a",
    ]
)
source_meta = source.meta_table
```

A cast fails when the target primitive is narrower than the boxed type. For example, a Python `float` becomes a [`java.lang.Double`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Double.html), which can't be cast directly to `float`. The following query fails with an error:

```python should-fail
from deephaven import empty_table

a = 3.14

source = empty_table(1).update(
    [
        "DoubleA = a",
        "FloatA = (float)a",
    ]
)
source_meta = source.meta_table
```

The error message reads:

```text
Value: table update operation failed. : Incompatible types; java.lang.Double cannot be converted to float
```

To get a `float` column, unbox the value to `double` first, then narrow it: `(float)(double)a`. The following query works:

```python order=source,source_meta
from deephaven import empty_table

a = 3.14

source = empty_table(1).update(["DoubleA = a", "FloatA = (float)(double)a"])
source_meta = source.meta_table
```

### Python functions

When a [query string calls a Python function](./python-functions.md), Deephaven sets the column type from the function's return [type hint](./python-functions.md#type-hints). A function whose hint maps to a Java type listed in [Data types](./data-types.md#java-data-types) needs no cast. Use a type cast when the function has no return [type hint](./python-functions.md#type-hints) or its hint has no Java mapping, such as `-> dict`.

Without a return [type hint](./python-functions.md#type-hints) or a cast, the result column isn't typed. For example, if `f` has no return type hint, `f(X)` gives a [`PyObject`](./pyobjects.md) column, and `f(X + 1)` gives a `java.lang.Object` column.

The function `random_symbol` in the following example has no return [type hint](./python-functions.md#type-hints), so a type cast converts its return value to a [`java.lang.String`](../reference/query-language/types/strings.md):

```python order=source
from deephaven import empty_table
from random import choice


def random_symbol():
    return choice(["AAPL", "GOOG", "MSFT"])


source = empty_table(10).update("Symbol = (String)random_symbol()")
```

### Python classes

Unlike [Python functions](./python-functions.md), methods of [Python classes](./python-classes.md) called in [query strings](./query-string-overview.md) do not use [type hints](./python-functions.md#type-hints) to set the column type. Instead, a method call produces a `java.lang.Object` column, so use a type cast when you need a specific column type for its result. The **Other objects to primitive** rule under [Type casts](#type-casts) makes a numeric cast such as `(int)` work on that `Object`.

The following example casts the results of a class method (`change_value`), a static method (`multiply_modulo`), and an instance method (`plus_modulo`):

```python order=source,source_meta
from deephaven import empty_table


class MyClass:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    @classmethod
    def change_value(cls, new_value):
        return new_value

    @staticmethod
    def multiply_modulo(x, y, modulo):
        if modulo == 0:
            return x * y
        return (x % modulo) * (y % modulo)

    def plus_modulo(self, modulo):
        if modulo == 0:
            return self.x + self.y
        return (self.x % modulo) + (self.y % modulo)


my_class = MyClass(15, 6)

source = empty_table(1).update(
    [
        "Q = MyClass.change_value(4)",
        "X = (int)MyClass.change_value(5)",
        "Y = (int)MyClass.multiply_modulo(11, 16, X)",
        "Z = (int)my_class.plus_modulo(X)",
    ]
)
source_meta = source.meta_table
```

Column `Q` has no cast, so `source_meta` shows its type as `java.lang.Object`. The casts give `X`, `Y`, and `Z` the `int` type.

## Related documentation

- [The Python-Java boundary](../conceptual/python-java-boundary.md)
- [Access metadata](./metadata.md)
- [Data types](./data-types.md)
- [Query string overview](./query-string-overview.md)
- [Query scope](./query-scope.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [Python classes in query strings](./python-classes.md)
- [PyObjects in tables](./pyobjects.md)
- [Casting](../reference/query-language/control-flow/casting.md)
- [Nulls](../reference/query-language/types/nulls.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`meta_table`](../reference/table-operations/metadata/meta_table.md)
- [Parse functions](../reference/query-language/query-library/auto-imported/parse.md)
- [`update`](../reference/table-operations/select/update.md)
- [`empty_table` Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.empty_table)
- [`meta_table` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.meta_table)
- [`update` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.update)
