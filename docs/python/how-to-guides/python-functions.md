---
title: Python functions in query strings
sidebar_label: Functions
---

The ability to use your own custom Python [variables](./python-variables.md), functions, and [classes](./python-classes.md) in Deephaven query strings is one of its most powerful features. The use of custom Python functions in query strings follows some basic rules, which are outlined in this guide.

## Call a Python function in a query string

Python functions can be called in query strings just like they can be called in Python code. They follow [query scope](./query-scope.md) rules, which are similar to Python's [LEGB](https://realpython.com/python-scope-legb-rule/) scoping. The following example calls a user-defined Python function in a query string to create a new column in a table:

```python order=source,source_meta
from deephaven import empty_table
from random import choice


def random_symbol() -> str:
    return choice(["AAPL", "GOOG", "MSFT"])


source = empty_table(10).update("Symbol = random_symbol()")
source_meta = source.meta_table
```

To help the Deephaven query engine infer the type of a column, you should use [type hints](#type-hints) or [type casts](./casting.md) to specify the function's return type. If the return type is not specified, Python function results will be classified as [`PyObjects`](./pyobjects.md).

### Type hints

Type hints in Python specify the expected data types returned from functions. For example:

```python
def calculate_price(quantity: int) -> float:
    return quantity * 1.99
```

Type hints in Deephaven queries:

- Ensure appropriate data types in columns.
- Eliminate the need for explicit [type casting](./casting.md).
- Improve query performance.
- Make code more maintainable.

The following example demonstrates a type-hinted function in a query string:

```python order=source,source_meta
from deephaven import empty_table
from random import choice


def random_symbol() -> str:
    return choice(["AAPL", "GOOG", "MSFT"])


source = empty_table(10).update("Symbol = random_symbol()")
source_meta = source.meta_table
```

### Optional return values

Functions can be designed to return `None` in certain cases using Python's `Optional` type hint. Deephaven properly handles these nullable return values in [query strings](./query-string-overview.md):

```python order=source
from deephaven import empty_table
from typing import Optional


def my_optional_return_func(value: int) -> Optional[int]:
    if value % 2 == 0:
        return None
    return 2 * value


source = empty_table(10).update("X = my_optional_return_func(ii)")
```

### Multiple return values

Python functions can return multiple values through tuple unpacking. Despite the fact that there is no Java equivalent, Deephaven supports functions that return multiple values through [jpy's](./use-jpy.md) [`org.jpy.PyObject`](./pyobjects.md) data type.

Consider the following example, which calls functions in [query strings](./query-string-overview.md) that return three values. One uses a [type hint](#type-hints) while the other does not. The `func_sum` function works in both cases:

```python order=source,source_meta
from typing import Tuple
from deephaven import empty_table


def func_tuple_typehint() -> Tuple[int, int, int]:
    return 1, 2, 3


def func_tuple_no_typehint():
    return 1, 2, 3


def func_sum(x) -> int:
    return sum(x)


source = empty_table(1).update(
    [
        "X1 = func_tuple_typehint()",
        "Y1 = func_sum(X1)",
        "X2 = func_tuple_no_typehint()",
        "Y2 = func_sum(X2)",
    ]
)
source_meta = source.meta_table
```

### Array return values

In Python, a sequence is an abstract base class that represents any ordered, indexable collection. It includes common types like lists, tuples, strings, arrays, and more.

Deephaven supports calling functions that return any sequence type in [query strings](./query-string-overview.md). The following example calls a Python function that returns a sequence of values in a [query string](./query-string-overview.md):

```python order=source
from deephaven import empty_table
from typing import Sequence
from random import choice


def random_symbols() -> Sequence[str]:
    return [choice(["AAPL", "GOOG", "MSFT"]) for _ in range(3)]


source = empty_table(10).update("Symbols = random_symbols()")
```

## Query language methods

Python functions can be used in conjunction with Deephaven's [built-in query language methods](./built-in-functions.md). The following example calls both a Python function and [built-in function](./built-in-functions.md) in the same query string:

```python order=source,source_meta
from deephaven import empty_table


def my_func(value: float) -> float:
    return value**0.5


source = empty_table(40).update(["X = 0.1 * ii", "Y = my_func(X) * cos(X)"])
source_meta = source.meta_table
```

## Related documentation

- [Built-in query language constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Python variables in query strings](./python-variables.md)
- [Python classes and objects in query strings](./python-classes.md)
- [PyObjects in tables](./pyobjects.md)
- [The Python-Java boundary](../conceptual/python-java-boundary.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`meta_table`](../reference/table-operations/metadata/meta_table.md)
- [`update`](../reference/table-operations/select/update.md)
