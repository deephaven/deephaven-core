---
title: Handle PyObjects in tables
---

This guide shows how to recognize [`org.jpy.PyObject`](https://jpy.readthedocs.io/en/latest/_static/java-apidocs/org/jpy/PyObject.html) columns in your tables, what limits them, when to use them, and how to avoid them. The rest of this guide calls them `PyObject` columns.

This data type most commonly arises from [Python functions called in query strings](./python-functions.md) without [type hints](./python-functions.md#type-hints) or [type casts](./casting.md). Two other sources also produce `PyObject` values:

- Attributes of [Python classes and objects](./python-classes.md), such as `MyClass.a` or `my_obj.b`.
- Python [variables](./python-variables.md) that hold tuples, [NumPy arrays](./use-numpy.md), or most NumPy scalars, such as `numpy.int64`.

Methods called through a [class or object](./python-classes.md) produce [`java.lang.Object`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html) columns instead. These columns have the same limits as `PyObject` columns and also need a [type cast](./casting.md).

Because a `PyObject` can hold any Python object, the engine can infer very little about it. As a result, `PyObject` columns support fewer operations and are [less performant](#performance) than [Java primitive](./data-types.md#primitive-types) columns. They are still useful in some situations. See [When to use PyObject columns](#when-to-use-pyobject-columns).

## What is a PyObject?

A [`PyObject`](https://jpy.readthedocs.io/en/latest/_static/java-apidocs/org/jpy/PyObject.html) is a Java wrapper around an arbitrary Python object. It comes from [jpy](./use-jpy.md), the bidirectional Python-Java bridge that Deephaven uses to call Python from [query strings](./query-string-overview.md). It is a flexible data type because it can hold any Python object. This flexibility comes at the cost of compatibility and speed.

Consider the following example, which calls three [Python functions](./python-functions.md) in [query strings](./query-string-overview.md) without any [type hints](./python-functions.md#type-hints). They return an `int`, `list`, and `dict`, respectively.

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

### Recognize a PyObject column

`source_meta` holds the [`meta_table`](../reference/table-operations/metadata/meta_table.md) of `source`. Its `DataType` column shows `org.jpy.PyObject` for all three columns. Because all three functions lack return [type hints](./python-functions.md#type-hints), the engine has no Java type to assign, so it stores each result as a `PyObject`.

To recognize a `PyObject` column in any table, check its [`meta_table`](../reference/table-operations/metadata/meta_table.md) for `org.jpy.PyObject` in the `DataType` column.

Without a return [type hint](./python-functions.md#type-hints) or a [cast](./casting.md), the result column isn't typed. The column type depends on the function's arguments:

- If every argument is a column, a variable, or a constant, as in `f(X)`, the column is a `PyObject` column.
- If an argument is an expression, as in `f(X + 1)`, the column is a [`java.lang.Object`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html) column.

A [`java.lang.Object`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html) column has the same limits as a `PyObject` column and takes the same remedies.

> [!TIP]
> Add [type hints](./python-functions.md#type-hints) to Python functions that you call in [query strings](./query-string-overview.md). See [How to avoid PyObject columns](#how-to-avoid-pyobject-columns) for this and other remedies.

## Limitations of PyObject columns

### Compatibility

Many of Deephaven's [built-in query language functions](./built-in-functions.md) do not support [`PyObject`](https://jpy.readthedocs.io/en/latest/_static/java-apidocs/org/jpy/PyObject.html) columns. [Built-in functions](./built-in-functions.md) that take [`java.lang.Object`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html) as input support `PyObject` columns because `PyObject` extends `Object`.

The following code calls the built-in [`sin`](/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) function on a `PyObject` column, and the query fails:

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

No [`sin`](/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) overload accepts a `PyObject` argument. To call [`sin`](/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) on these values, [cast](./casting.md) them in the formula, as in `Y = sin((double)X)`. You can also add a [type hint](./python-functions.md#type-hints) to the function. See [How to avoid PyObject columns](#how-to-avoid-pyobject-columns).

### Performance

`PyObject` columns are less performant than [Java primitive](./data-types.md#primitive-types) columns for two reasons:

- Each cell is a Java object that references a separate Python object. These columns use more memory than primitive columns, which store their values in primitive arrays.
- Almost every operation on these values, such as passing them to a [Python function](./python-functions.md) or [casting](./casting.md) them, crosses the [Python-Java boundary](../conceptual/python-java-boundary.md).

## When to use PyObject columns

Despite these [limitations](#limitations-of-pyobject-columns), [`PyObject`](https://jpy.readthedocs.io/en/latest/_static/java-apidocs/org/jpy/PyObject.html) columns are useful when:

- Other [Python functions](./python-functions.md) read the column's values and perform operations that no [built-in query language function](./built-in-functions.md) supports.
- A [Python function](./python-functions.md) returns a data type that Deephaven doesn't convert to a Java type, such as a `dict` or an instance of a custom class.
- A [Python function](./python-functions.md) returns different data types depending on the input.

The following example shows all three cases. `get_player` returns a `dict`, and a second [Python function](./python-functions.md) named `best_score` reads that `dict`. `get_reading` returns an `int`, a `str`, or a `float`, depending on the row:

```python order=source,source_meta
from deephaven import empty_table

players = [
    {"name": "Ann", "scores": [7, 9, 4]},
    {"name": "Bo", "scores": [5, 8]},
    {"name": "Cy", "scores": [6]},
]
readings = [42, "n/a", 3.5]


def get_player(i):
    return players[i]


def best_score(player) -> int:
    return max(player["scores"])


def get_reading(i):
    return readings[i]


def type_name(value) -> str:
    return type(value).__name__


source = empty_table(3).update(
    [
        "Player = get_player(ii)",
        "BestScore = best_score(Player)",
        "Reading = get_reading(ii)",
        "ReadingType = type_name(Reading)",
    ]
)
source_meta = source.meta_table
```

`source_meta` shows that `Player` and `Reading` are `PyObject` columns. `BestScore` and `ReadingType` are `long` and [`java.lang.String`](../reference/query-language/types/strings.md) columns, respectively, because `best_score` and `type_name` have return [type hints](./python-functions.md#type-hints). The `ReadingType` column shows that the `Reading` column holds a different Python type in each row.

## How to avoid PyObject columns

For better performance and type safety, minimize the use of [`PyObject`](https://jpy.readthedocs.io/en/latest/_static/java-apidocs/org/jpy/PyObject.html) columns in your tables. Keep them only for the cases listed in [When to use PyObject columns](#when-to-use-pyobject-columns).

Use whichever of these remedies matches where the `PyObject` values come from:

- **Add [type hints](./python-functions.md#type-hints) to your functions**: Deephaven uses the return type hint to convert return values to the matching Java type. A hint on a tuple or list works too when it names a supported element type. For example, `tuple[int, int, int]` produces a Java `long[]` column. See [Array return values](./python-functions.md#array-return-values) and [Multiple return values](./python-functions.md#multiple-return-values).

  Deephaven has no Java type for the following hints, so they still produce a `PyObject` column:

  - `-> dict`
  - `-> object`
  - `-> MyClass`, where `MyClass` is a custom class
  - `-> list` or `-> tuple` with no element type
  - `-> list[dict]`, `-> list[MyClass]`, or another sequence of `dict` or custom-class elements

- **Use explicit [type casts](./casting.md)**: Cast the value with a Java-style [cast](./casting.md), such as `(int)` or `(String)`, when a type hint can't help:
  - The value is an attribute or method of a [Python class or object](./python-classes.md). Deephaven ignores [type hints](./python-functions.md#type-hints) on these.
  - The function is one you can't edit.
  - The value is a [NumPy](./use-numpy.md) scalar variable, such as a `numpy.int64`. You can also convert it with Python's `int`, `float`, or `bool` before you use it.

- **Convert tuple and [NumPy array](./use-numpy.md) variables to Java arrays**: Use [`jpy.array`](./use-jpy.md#create-java-arrays) or [`deephaven.dtypes.array`](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.array) to convert the variable before you use it in a [query string](./query-string-overview.md). See [Sequences](./python-variables.md#sequences) and [Work with arrays](./work-with-arrays.md).

The following code block uses the first two remedies: a [type hint](./python-functions.md#type-hints) on `my_func` and a [`(double)` cast](./casting.md) on the result of `my_func_without_type_hint`. The resulting `Y` and `Z` columns are both `double` columns:

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

- [The Python-Java boundary](../conceptual/python-java-boundary.md)
- [Query string overview](./query-string-overview.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Casting](./casting.md)
- [Data types](./data-types.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [Python classes and objects in query strings](./python-classes.md)
- [How to use jpy](./use-jpy.md)
- [Use NumPy in Deephaven queries](./use-numpy.md)
- [Triage errors](./triage-errors.md)
- [Work with arrays](./work-with-arrays.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`meta_table`](../reference/table-operations/metadata/meta_table.md)
- [`update`](../reference/table-operations/select/update.md)
- [`empty_table` Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.empty_table)
- [`meta_table` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.meta_table)
- [`update` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.update)
- [`deephaven.dtypes.array` Pydoc](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.array)
- [`jpy.array` Pydoc](/core/pydoc/code/jpy.html#jpy.array)
- [`sin` Javadoc](/core/javadoc/io/deephaven/function/Numeric.html#sin(double))
- [`PyObject` Javadoc](https://jpy.readthedocs.io/en/latest/_static/java-apidocs/org/jpy/PyObject.html)
