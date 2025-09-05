---
title: Casting
sidebar_label: Casting
---

This guide covers casting in Deephaven. Data type conversion, or casting for short, is the process of changing a value from one data type to another.

In tables, casting is essential to ensure that columns are in the correct format for subsequent operations. It's also vital for optimizing performance and managing memory effectively.

For more information on why casting is important in Python queries, see:

- [The Python-Java boundary](../conceptual/python-java-boundary.md)
- [PyObjects in tables](./pyobjects.md)

## Type casts

A type cast is an expression that converts a value from one data type to another. In Python, type casts use the syntax:

```python order=null
a = 3
b = float(a)
```

In Deephaven, type casts in [query strings](./query-string-overview.md) ensure that columns convert the data type of a column to another. They use [Java syntax](https://www.w3schools.com/java/java_type_casting.asp).

### Python variables

The following query creates a column using a [Python variable](./python-variables.md). The column is automatically converted to the `byte` type. A type cast is used to convert the variable to a Java primitive `int`:

```python order=source,source_meta
from deephaven import empty_table

a = 1

source = empty_table(1).update(
    [
        "BytePi = a",
        "IntPi = (int)a",
    ]
)
source_meta = source.meta_table
```

Type casting [Python variables in query strings](./python-variables.md) has limitations. Unlike Python, where basic data types have arbitrary precision, Java primitives have fixed precision. You cannot cast a value to a type with smaller precision. For example, the following query fails with an error:

```python should-fail
from deephaven import empty_table

a = 3.14

source = empty_table(1).update(
    [
        "DoublePi = a",
        "FloatPi = (float)a",
    ]
)
source_meta = source.meta_table
```

```text
Value: table update operation failed. : Incompatible types; java.lang.Double cannot be converted to float
```

The Python `float` type corresponds most closely to the Java `double` type in terms of precision. When a Python `float` is used in a query string, Deephaven treats it as a boxed `java.lang.Double` object until it's written to the table. Since the Deephaven engine lacks complete type information about variable `a`, it cannot safely downcast it from a `double` to the lower-precision `float` type.

### Python functions

[Python functions called in query strings](./python-functions.md) should generally use [type hints](./python-functions.md#type-hints) rather than type casts. However, type casts still work and can be used when needed.

The following example uses a type cast to convert the returned value of a Python function to a [`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html):

> [!NOTE]
> The shorthand notation `(String)` is used below. The full notation is `(java.lang.String)`.

```python order=source
from deephaven import empty_table
from random import choice


def random_symbol():
    return choice(["AAPL", "GOOG", "MSFT"])


source = empty_table(10).update("Symbol = (String)random_symbol()")
```

### Python classes

Unlike [Python functions](./python-functions.md), [Python classes](./python-classes.md) used in [query strings](./query-string-overview.md) cannot leverage [type hints](./python-functions.md#type-hints) to specify return types. Instead, you must use explicit type casts to ensure columns have the appropriate data types. The example below demonstrates using type casts with Python class methods and attributes in query strings:

```python order=source,source_meta
from deephaven import empty_table
import numpy as np


class MyClass:
    my_value = 3

    def __init__(self, x, y):
        self.x = x
        self.y = y

    @classmethod
    def change_value(cls, new_value) -> np.intc:
        MyClass.my_value = new_value
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

## Related documentation

- [Access metadata](./metadata.md)
- [Query string overview](./query-string-overview.md)
- [Query scope](./query-scope.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [Python classes in query strings](./python-classes.md)
- [PyObjects in tables](./pyobjects.md)
- [The Python-Java boundary](../conceptual/python-java-boundary.md)
