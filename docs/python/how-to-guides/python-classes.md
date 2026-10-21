---
title: Python classes and objects in query strings
---

Using your own Python [variables](./python-variables.md), [functions](./python-functions.md), classes, and objects in Deephaven [query strings](./query-string-overview.md) is one of Deephaven's most powerful features. This guide shows how to use class variables, instance variables, and class, static, and instance methods, and how to give the resulting columns the right [data types](./data-types.md).

> [!CAUTION]
> Python classes and objects in [query strings](./query-string-overview.md) have [performance and memory implications](../conceptual/python-java-boundary.md). Python objects live outside the Java heap, so they can significantly increase the process's resident memory. For performance-critical or high-throughput queries, consider using [built-in query language functions](./built-in-functions.md) instead.

## Class and instance variables in query strings

A class is a blueprint for objects: it defines the data and behavior that each object created from it has. Classes can have class variables, which belong to the class itself, and instance variables, which belong to each object.

In the example below, `a` is a class variable and `b` is an instance variable. You can use `a` without an instance of the class, but `b` requires one. In the [query string](./query-string-overview.md), access `a` through the class `MyClass` and `b` through the instance `my_class`:

```python order=source,source_meta
from deephaven import empty_table


class MyClass:
    a = 1

    def __init__(self, b):
        self.b = b


my_class = MyClass(2)

source = empty_table(1).update(["X = MyClass.a", "Y = my_class.b"])
source_meta = source.meta_table
```

In the example above, `source_meta` shows that the `X` and `Y` columns are of type [`org.jpy.PyObject`](./pyobjects.md). The engine reads class and instance variables from the Python object at run time, so it can't tell their types when it compiles the [query string](./query-string-overview.md). A type annotation such as `a: int = 1` doesn't change this.

Top-level [Python variables](./python-variables.md) behave differently, because Deephaven sets their column types from their values. To give a class or instance variable a specific column type, use a [type cast](./casting.md#type-casts).

The following example adds [type casts](./casting.md#type-casts) that make `X` an `int` column and `Y` a `double` column:

```python order=source,source_meta
from deephaven import empty_table


class MyClass:
    a = 1

    def __init__(self, b):
        self.b = b


my_class = MyClass(2.1)

source = empty_table(1).update(["X = (int)MyClass.a", "Y = (double)my_class.b"])
source_meta = source.meta_table
```

In `source_meta`, `X` is now an `int` column and `Y`, which holds the instance value `2.1`, is a `double` column.

## Methods in query strings

In Python, classes can have three types of methods:

- **Class methods**: They use the `@classmethod` decorator and take `cls` as the first parameter. They operate on the class itself and can access class variables.
- **Static methods**: They use the `@staticmethod` decorator and take neither `self` nor `cls`, so they receive no implicit reference to the instance or class.
- **Instance methods**: They use no decorator and take `self` as the first parameter. They require an instance of the class and can access instance variables.

[Query strings](./query-string-overview.md) support all three.

The following example calls a class method, a static method, and an instance method in a [query string](./query-string-overview.md). It calls the instance method on `my_class`, an instance of `MyClass`. Deephaven ignores [type hints](./python-functions.md#type-hints) on methods called through a class or object, so the example uses [type casts](./casting.md#type-casts) to give each column a [Java primitive type](./data-types.md#primitive-types):

```python order=source,source_meta
from deephaven import empty_table


class MyClass:
    my_value = 3

    def __init__(self, x, y):
        self.x = x
        self.y = y

    @classmethod
    def scaled_value(cls, factor):
        return cls.my_value * factor

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
        "X = (int)MyClass.scaled_value(5)",
        "Y = (int)MyClass.multiply_modulo(11, 16, X)",
        "Z = (int)my_class.plus_modulo(X)",
    ]
)
source_meta = source.meta_table
```

In `source`, `scaled_value` multiplies the class variable `my_value`, which is `3`, by `5`, so `X` is `15`. `Y` and `Z` use `X` as the modulus, giving `11` and `6`.

In `source_meta`, the casts make `X`, `Y`, and `Z` columns of type `int`. Without a cast, a method call produces a `java.lang.Object` column.

## Related documentation

- [Python-Java boundary](../conceptual/python-java-boundary.md)
- [Query strings overview](./query-string-overview.md)
- [Query scope](./query-scope.md)
- [Query language built-in constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Formulas in query strings](./formulas.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [PyObjects in tables](./pyobjects.md)
- [Casting](./casting.md)
- [Data types in Deephaven and Python](./data-types.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`meta_table`](../reference/table-operations/metadata/meta_table.md)
- [`update`](../reference/table-operations/select/update.md)
- [`empty_table` Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.empty_table)
- [`meta_table` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.meta_table)
- [`update` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.update)
