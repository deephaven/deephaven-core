---
title: Python functions in query strings
---

The ability to use your own custom Python [variables](./python-variables.md), functions, and [classes](./python-classes.md) in [query strings](./query-string-overview.md) is one of Deephaven's most powerful features. This guide covers the basic rules for using custom Python functions in [query strings](./query-string-overview.md).

> [!CAUTION]
> Python functions in [query strings](./query-string-overview.md) have [performance and memory implications](../conceptual/python-java-boundary.md). Python objects live outside the Java heap, so the process's resident memory can grow well beyond the heap size. For performance-critical or high-throughput queries, consider using [built-in query language functions](./built-in-functions.md) instead.

## Call a Python function in a query string

You call a Python function in a [query string](./query-string-overview.md) by name, passing arguments by position, just as you would in Python code. Deephaven finds the function through [query scope](./query-scope.md) rules, which are similar to Python's [LEGB](https://realpython.com/python-scope-legb-rule/) scoping, with [some differences](./query-scope.md#query-scope-in-python). For a function that takes a column value, see [Parameter type hints](#parameter-type-hints). The following example calls a user-defined function with no arguments to create a new column in a table:

```python order=source,source_meta
from deephaven import empty_table
from random import choice


def random_symbol():
    return choice(["AAPL", "GOOG", "MSFT"])


source = empty_table(10).update("Symbol = random_symbol()")
source_meta = source.meta_table
```

The `random_symbol` function has no return type hint. [`meta_table`](../reference/table-operations/metadata/meta_table.md) lists each column's name and type, and `source_meta` shows that `Symbol` is a [`PyObject`](./pyobjects.md) column. A [`PyObject`](./pyobjects.md) is a Java wrapper around a Python object. To get a typed column instead, add a [type hint](#type-hints) to the function, or [cast](./casting.md) its result in the [query string](./query-string-overview.md).

## Type hints

Type hints in Python specify the expected data types of a function's parameters and return value. Deephaven uses them in two ways:

- Parameter hints tell Deephaven how to convert the arguments passed to the function.
- The return hint sets the type of the result column.

For example, a return hint of `int` produces a Java `long` column, `float` produces a `double` column, and `str` produces a [`String`](../reference/query-language/types/strings.md) column. For the full mapping, see [Data types](./data-types.md#java-data-types).

Type hints in Deephaven queries:

- Ensure appropriate data types in columns.
- Eliminate the need for explicit [type casting](./casting.md).
- Avoid [`PyObject`](./pyobjects.md) columns, which are slower and use more memory than typed columns.
- Make code more maintainable.

The following example adds the `-> str` return type hint to the `random_symbol` function from the previous example, so `source_meta` shows that `Symbol` is a [`String`](../reference/query-language/types/strings.md) column:

```python order=source,source_meta
from deephaven import empty_table
from random import choice


def random_symbol() -> str:
    return choice(["AAPL", "GOOG", "MSFT"])


source = empty_table(10).update("Symbol = random_symbol()")
source_meta = source.meta_table
```

### Optional return values

Python's `Optional` type hint declares that a function can return `None`. When you call such a function in a [query string](./query-string-overview.md), Deephaven stores each `None` return as a [null value](./null-inf-nan.md). In the following example, `source_meta` shows that `X` is a `long` column, and every row where [`ii`](../reference/query-language/variables/special-variables.md) is even holds a [null value](./null-inf-nan.md):

```python order=source,source_meta
from deephaven import empty_table
from typing import Optional


def my_optional_return_func(value: int) -> Optional[int]:
    if value % 2 == 0:
        return None
    return 2 * value


source = empty_table(10).update("X = my_optional_return_func(ii)")
source_meta = source.meta_table
```

### Array return values

In Python, a sequence is any ordered, indexable collection, such as a list or a tuple. Deephaven stores a function's result in a Java [array](./work-with-arrays.md) column when the function's return type hint is one of the following:

- A sequence with an element type, such as `Sequence[str]` or `list[int]`.
- A NumPy array with a dtype, such as `numpy.typing.NDArray[np.int64]`.

The element type sets the array type:

| Element type                                | Column type                                                              |
| ------------------------------------------- | ------------------------------------------------------------------------ |
| `int`, `np.int64`                           | `long[]`                                                                 |
| `np.int32`                                  | `int[]`                                                                  |
| `np.int16`                                  | `short[]`                                                                |
| `np.int8`                                   | `byte[]`                                                                 |
| `float`, `np.float64`                       | `double[]`                                                               |
| `np.float32`                                | `float[]`                                                                |
| `bool`, `np.bool_`                          | `Boolean[]`                                                              |
| `str`, `np.str_`                            | [`String`](../reference/query-language/types/strings.md) `[]`            |
| `datetime`, `pd.Timestamp`, `np.datetime64` | [`Instant`](../reference/query-language/types/date-time.md#instant) `[]` |
| `np.uint16`                                 | `char[]`                                                                 |
| Any other type                              | [`PyObject`](./pyobjects.md)                                             |

Three other kinds of return hint aren't in the table:

- A Deephaven type from [`deephaven.dtypes`](/core/pydoc/code/deephaven.dtypes.html), such as `dtypes.int32`, maps the same way as its NumPy type.
- A `bytes` or `bytearray` return hint produces a `byte[]` column.
- A sequence hint with no element type, such as plain `list`, produces a [`PyObject`](./pyobjects.md) column.

The following example calls `random_symbols`, which returns a sequence of strings, in a [query string](./query-string-overview.md). `source_meta` shows that `Symbols` is a [`String`](../reference/query-language/types/strings.md) `[]` column:

```python order=source,source_meta
from deephaven import empty_table
from collections.abc import Sequence
from random import choice


def random_symbols() -> Sequence[str]:
    return [choice(["AAPL", "GOOG", "MSFT"]) for _ in range(3)]


source = empty_table(10).update("Symbols = random_symbols()")
source_meta = source.meta_table
```

### Multiple return values

Python functions can return multiple values as a tuple. Java has no tuple type, so the return type hint decides how Deephaven stores the result:

- Without a hint, the column holds [`PyObject`](./pyobjects.md) values, as in the [first example](#call-a-python-function-in-a-query-string).
- With a hint such as `tuple[int, int, int]`, Deephaven treats the tuple as a sequence and stores it in an [array](./work-with-arrays.md) column, as described in [Array return values](#array-return-values).

The first type argument sets the [array's](./work-with-arrays.md) element type, so `tuple[int, int, int]` produces a `long[]` column. Every element must have that type, so a `float` element in this hint, even `2.0`, makes the query fail.

The following example calls two functions that each return three values in a [query string](./query-string-overview.md). `func_tuple_typehint` has a `tuple[int, int, int]` return hint, so `X1` is a `long[]` column. `func_tuple_no_typehint` has no hint, so `X2` is a [`PyObject`](./pyobjects.md) column. `func_sum` accepts either column, so `Y1` and `Y2` both hold `6`:

```python order=source,source_meta
from deephaven import empty_table


def func_tuple_typehint() -> tuple[int, int, int]:
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

### Parameter type hints

A parameter type hint tells Deephaven which argument types the function accepts. When Deephaven compiles the [query string](./query-string-overview.md), it checks each argument against the hint:

- Deephaven accepts an argument whose Java type converts to the hinted type without loss. An `int` hint is a Java `long`, so it accepts `long` values such as [`ii`](../reference/query-language/variables/special-variables.md) and `int` values such as [`i`](../reference/query-language/variables/special-variables.md).
- Any other argument type raises an error. For example, you can't pass a `double` column to an `int` hint.

Deephaven converts each accepted argument to the hinted Python type before it calls the function. In the following example, both [`ii`](../reference/query-language/variables/special-variables.md) and [`i`](../reference/query-language/variables/special-variables.md) arrive in `times_two` as a Python `int`, and the `-> int` return hint makes `X` and `Y` `long` columns:

```python order=source,source_meta
from deephaven import empty_table


def times_two(value: int) -> int:
    return 2 * value


source = empty_table(10).update(["X = times_two(ii)", "Y = times_two(i)"])
source_meta = source.meta_table
```

A parameter hint also decides how a [null value](./null-inf-nan.md) reaches the function:

- Without `Optional`, Deephaven passes the [null value](./null-inf-nan.md) of a primitive column as is, so a null `long` arrives in Python as the integer `-9223372036854775808`.
- With an `Optional` parameter hint, such as `value: Optional[int]`, Deephaven passes each [null value](./null-inf-nan.md) as `None`.

In the following example, `X` uses the built-in constant [`NULL_LONG`](./built-in-constants.md#null-constants) to hold a [null value](./null-inf-nan.md) in every row where [`ii`](../reference/query-language/variables/special-variables.md) is even. `is_none` never receives `None`, so `IsNone` is `false` in every row. `is_none_optional` receives `None` for each null, so `IsNoneOptional` is `true` in the rows where `X` is null:

```python order=source
from deephaven import empty_table
from typing import Optional


def is_none(value: int) -> bool:
    return value is None


def is_none_optional(value: Optional[int]) -> bool:
    return value is None


source = empty_table(10).update(
    [
        "X = ii % 2 == 0 ? NULL_LONG : ii",
        "IsNone = is_none(X)",
        "IsNoneOptional = is_none_optional(X)",
    ]
)
```

## Combine with built-in functions

You can use Python functions alongside Deephaven's [built-in query language functions](./built-in-functions.md). The following example calls the Python function `my_func` and the built-in [`cos`](/core/javadoc/io/deephaven/function/Numeric.html#cos(double)) function in the same [query string](./query-string-overview.md). Because `my_func` returns a `float`, `source_meta` shows that `Y` is a `double` column:

```python order=source,source_meta
from deephaven import empty_table


def my_func(value: float) -> float:
    return value**0.5


source = empty_table(40).update(["X = 0.1 * ii", "Y = my_func(X) * cos(X)"])
source_meta = source.meta_table
```

## Related documentation

- [The Python-Java boundary](../conceptual/python-java-boundary.md)
- [Query strings overview](./query-string-overview.md)
- [Query scope](./query-scope.md)
- [Built-in query language constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Python variables in query strings](./python-variables.md)
- [Python classes and objects in query strings](./python-classes.md)
- [PyObjects in tables](./pyobjects.md)
- [Data types in Deephaven and Python](./data-types.md)
- [Null, NaN, and infinity values](./null-inf-nan.md)
- [Arrays](./work-with-arrays.md)
- [Casting](./casting.md)
- [Use Numba in Deephaven queries](./use-numba.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`meta_table`](../reference/table-operations/metadata/meta_table.md)
- [`update`](../reference/table-operations/select/update.md)
- [Special variables and constants](../reference/query-language/variables/special-variables.md)
- [Strings](../reference/query-language/types/strings.md)
- [Date-time](../reference/query-language/types/date-time.md)
- [`deephaven.dtypes` Pydoc](/core/pydoc/code/deephaven.dtypes.html)
- [`cos` Javadoc](/core/javadoc/io/deephaven/function/Numeric.html#cos(double))
