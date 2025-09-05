---
title: Handle PyObjects in tables
sidebar_label: PyObjects
---

This guide provides a comprehensive overview of the [`org.jpy.PyObject`](https://jpy.readthedocs.io/en/latest/_static/java-apidocs/index.html) data type, including its appearance in tables, its uses, limitations, and best practices.

This data type most commonly arises from [Python functions called in query strings](./python-functions.md) without [type hints](./python-functions.md#type-hints) or [type casts](./casting.md).

Since these objects can hold any arbitrary Python object, the Deephaven engine can infer very little about them. Thus, the supported operations on them are limited. Additionally, these columns are less performant than Java primitive columns. Despite this, there are still situations where these column types are useful. They are [outlined](#when-to-use-pyobjects) below.

This data type is generally referred to as `PyObject` for the remainder of this guide for brevity.

## What is a PyObject?

A `PyObject` is a Java wrapper around an arbitrary Python object. It is a product of [jpy](./use-jpy.md), the bidirectional Python-Java bridge used by Deephaven to facilitate calling Python from [query strings](./query-string-overview.md). It is a highly flexible data type because it can hold many different Python data types. This flexibility comes at the cost of compatibility and speed.

Consider the following example, which calls three Python functions in [query strings](./query-string-overview.md) without any [type hints](./python-functions.md#type-hints). They return an `int`, `list`, and `dict`, respectively.

> [!WARNING]
> It is best practice to use [type hints](./python-functions.md#type-hints) in Python functions, especially those called in [query strings](./query-string-overview.md).

```python order=source,source_meta
from deephaven import empty_table


def func_return_scalar():
    return 3


def func_return_list():
    return [1, 2, 3]


def func_return_dict():
    return {"A": 1, "B": 2, "C": 3}


source = empty_table(1).update(
    ["A = func_return_scalar()", "B = func_return_list()", "C = func_return_dict()"]
)
source_meta = source.meta_table
```

The `source_meta` table shows that all three columns are `PyObject` columns. Because all three functions lack [type hints](./python-functions.md#type-hints), the query engine does not have sufficient information to safely infer the returned data type. Therefore, the safe option is to return the `PyObject` type.

## When to use PyObjects

`PyObject` columns have [limitations](#limitations-of-pyobject-columns), but are useful in some cases. They are best used when:

- The Python function performs operations not supported by any [built-in query language functions](./built-in-functions.md).
- A Python function returns a data type with no Java analogue.
- A Python function may return different data types dependent on the input.

The following example demonstrates using Python's [tuple](https://docs.python.org/3/library/functions.html#func-tuple) data type, which has no direct Java equivalent. Thus, `PyObject` columns are useful because they make calling these [functions in query strings](./python-functions.md) simple:

```python order=source
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

## Limitations of PyObject columns

### Compatibility

Many of Deephaven's [built-in query language functions](./built-in-functions.md) do not support `PyObject` columns. Methods that take [`java.lang.Object`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html) as input support `PyObject` columns, since `PyObject` extends `Object`.

The following code attempts to call the built-in [`sin`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) function on a `PyObject` column, but fails, since there is no overloaded method with that name that takes the proper data type as input:

```python should-fail
from deephaven import empty_table


def func_without_type_hint(value):
    return float(value)


source = empty_table(40).update("X = func_without_type_hint(ii)")
result = source.update("Y = sin(X)")
```

The [stack trace](./triage-errors.md) contains the following line:

```text
Value: table update operation failed. : Cannot find method sin(org.jpy.PyObject)
```

There is no method that can handle an input data type of `PyObject`. Therefore, the query engine raises an error.

### Memory management

Because `PyObject` is a boxed type, columns of this type are less performant than Java primitive columns.

### Performance

`PyObject` columns are less performant than Java primitive columns for a couple of reasons:

- They are boxed types, which means that they require additional memory to store the data.
- Their use cases are limited almost entirely to [Python functions in query strings](./python-functions.md), which must cross the [Python-Java boundary](../conceptual/python-java-boundary.md).

## How to avoid PyObject columns

For better performance and type safety, minimize the use of PyObject columns in your tables. These columns are only necessary when you specifically need to work with complex Python objects that don't have Java equivalents.

To prevent automatic PyObject creation when using Python functions in query strings:

1. **Add type hints to your functions**: This allows Deephaven to convert return values to appropriate Java types
2. **Use explicit type casts**: When type hints aren't possible, use Java-style casts like `(int)` or `(String)`

The following code block uses both to [cast](./casting.md) columns to appropriate Java primitive types:

```python order=source,source_meta
from deephaven import empty_table


def my_func(x: float) -> float:
    return x**0.5


def my_func_without_type_hint(x):
    return x**0.5


source = empty_table(10).update(
    [
        "X = 0.1 * ii",
        "Y = my_func(X)",
        "Z = (double)my_func_without_type_hint(X)",
    ]
)
source_meta = source.meta_table
```

## Related documentation

- [Built-in query language constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [The Python-Java boundary](../conceptual/python-java-boundary.md)
- [How to use jpy](./use-jpy.md)
