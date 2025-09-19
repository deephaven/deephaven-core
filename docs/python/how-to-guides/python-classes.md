---
title: Python classes and objects in query strings
sidebar_label: Classes & Objects
---

The ability to use your own custom Python [variables](./python-variables.md), [functions](./python-functions.md), classes, and objects in Deephaven query strings is one of its most powerful features. The use of Python classes in query strings follows some basic rules, which are outlined in this guide.

> [!IMPORTANT]
> Deephaven does not currently support Python [type hints](./python-functions.md#type-hints) for classes. If you want to ensure that the data types of class variables and methods are correct, you must use [type casts](./casting.md) in the query strings.

## Classes and objects in Python

Python is an object-oriented programming language. Its design revolves around the concept of "objects", which can contain arbitrary data and code suited to any given task.

In Python, everything is an object. Even scalar data types are objects:

```python order=:log
my_int = 1

print(type(my_int))
```

Classes are blueprints for objects. They define the properties and behaviors that the objects created from the class will have.

### Variables

Class variables can be either static or instance variables. In the example below, `a` is a static class variable and `b` is an instance variable. The variable `a` does _not_ require an instance of the class to be used, while `b` does. Notice how in the query string `a` is accessed using the actual class `MyClass`, while `b` must be used with the instance of `MyClass` called `my_class`:

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

In the example above, `source_meta` shows that the `X` and `Y` columns are of type [`org.jpy.PyObject`](./pyobjects.md). Without [type casts](./casting.md#type-casts), the engine cannot infer an appropriate data type.

The following example adds [type casts](./casting.md#type-casts) to ensure columns are of an appropriate Java primitive type:

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

### Functions (methods)

In Python, classes can have three types of methods:

- **Class methods**: Defined with the `@classmethod` decorator and take `cls` as the first parameter. They operate on the class itself and can access class variables.
- **Instance methods**: Defined with the `self` parameter and require an instance of the class to exist in order to be called.
- **Static methods**: Defined with the `@staticmethod` decorator and do not take `self` or `cls` as a parameter. They cannot access class or instance variables.

All three are supported in query strings.

The following code block calls a static, class, and instance method in a query string. Note how the instance method is called on the instance of `MyClass` called `my_class`. [Type casts](./casting.md) are used to ensure the resultant columns are of an appropriate Java primitive type:

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
    def multiply_modulo(x, y, modulo) -> np.double:
        if modulo == 0:
            return x * y
        return (x % modulo) * (y % modulo)

    def plus_modulo(self, modulo) -> np.intc:
        if modulo == 0:
            return self.x + self.y
        return (self.x % modulo) + (self.y % modulo)


my_class = MyClass(15, 6)

source = empty_table(1).update(
    [
        "X = (int)MyClass.change_value(5)",
        "Y = (int)MyClass.multiply_modulo(11, 16, X)",
        "Z = (int)my_class.plus_modulo(X)",
    ]
)
source_meta = source.meta_table
```

## Related documentation

- [Built-in query language constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Filters in query strings](./filters.md)
- [Formulas in query strings](./formulas.md)
- [Operators in query strings](./operators.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [PyObjects in tables](./pyobjects.md)
- [Python-Java boundary](../conceptual/python-java-boundary.md)
