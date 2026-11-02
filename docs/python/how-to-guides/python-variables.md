---
title: Python variables in query strings
---

Using your own Python variables, [functions](./python-functions.md), and [classes](./python-classes.md) in Deephaven [query strings](./query-string-overview.md) is one of Deephaven's most powerful features. This guide explains how the engine finds your variables and handles each kind of Python value, and when you need [type casts](./casting.md#type-casts). Its examples cover scalars, strings, sequences, and dicts.

## How the engine finds and converts variables

The engine finds each variable by [query scope](./query-scope.md) rules. It looks up a name in two places, in this order:

1. The local scope of the function that calls the table operation.
2. The global scope of that function's module.

This is close to Python's own [LEGB rule](https://realpython.com/python-scope-legb-rule/), with two differences:

- The engine doesn't search Python's built-in names, such as `print` or `range`.
- The engine sees an enclosing function's variable only if the inner function's own code also refers to that variable.

[Query strings](./query-string-overview.md) can use Deephaven's own [built-in functions, constants, and variables](./query-scope.md#built-in-functions-constants-and-variables) without your defining them in Python.

A [query string](./query-string-overview.md) can't use a Python variable whose name is one of the following, because the engine skips it when it compiles the query string:

- A Java keyword or literal, such as `int`, `double`, `new`, `true`, or `null`.
- One of the query language's [special variables](../reference/query-language/variables/special-variables.md): `i`, `ii`, or `k`.

A variable with the same name as a column of the table is hidden too. The [query string](./query-string-overview.md) reads the column instead, with no error. See [Columns take precedence over variables](./query-scope.md#columns-take-precedence-over-variables).

The Deephaven engine is written in Java, so every variable a [query string](./query-string-overview.md) uses crosses the [Python-Java boundary](../conceptual/python-java-boundary.md). The variable's type determines what the engine does with it:

- Python scalars and strings: the engine converts a `bool` to a Java `Boolean`, an `int` to a Java integer type, a `float` to a `double`, and a `str` to a [`String`](../reference/query-language/types/strings.md). See [Scalars](#scalars) and [Strings](#strings).
- Python lists and dicts: the engine wraps them as Java `List` and `Map` objects. See [Sequences](#sequences) and [Dicts](#dicts).
- Functions and other callables: the engine wraps them so [query strings](./query-string-overview.md) can call them. See [Python functions in query strings](./python-functions.md).
- Java objects, such as arrays created with [jpy](./use-jpy.md): the engine uses them unchanged.
- Other objects, such as tuples, [NumPy arrays](./use-numpy.md), and most [NumPy scalars](#scalars): the engine passes them as [PyObjects](./pyobjects.md).

Each example below also creates a [`meta_table`](../reference/table-operations/metadata/meta_table.md) so you can see the column types Deephaven assigns.

## Scalars

Scalars are single values, such as numbers or booleans. They work in a [query string](./query-string-overview.md) with no special syntax: refer to a scalar variable by its name. The following example uses Python variables in [query strings](./query-string-overview.md) to create new columns in a table:

```python order=source,source_meta
from deephaven import empty_table

a = 4
b = 3.14
c = -1.91e7
d = True

source = empty_table(1).update(["A = a", "B = b", "C = c + 1", "D = d"])
source_meta = source.meta_table
```

`source_meta` shows the Java type each Python value becomes:

- A Python `int` becomes the smallest Java integer type (`byte`, `short`, `int`, or `long`) that holds its value. Column `A` holds `4`, so it is a `byte` column.
- A Python `float` becomes a `double`, so columns `B` and `C` are `double` columns.
- A Python `bool` becomes a `java.lang.Boolean`, so column `D` is a `java.lang.Boolean` column.

Because the column type of a Python `int` depends on its value, [cast](./casting.md#python-variables) the variable when you need a fixed type, for example `"A = (long)a"`. See [Casts with Python variables](./casting.md#python-variables).

If the value equals the [null constant](../reference/query-language/types/nulls.md) of the type the engine picks, such as `-128` for a `byte`, the column reads it as null. A [cast](./casting.md#type-casts) doesn't change that, because the conversion to the smaller type happens before the cast widens the value.

Most [NumPy](./use-numpy.md) scalars, such as `numpy.int64`, `numpy.float32`, and `numpy.bool_`, aren't converted. They reach the [query string](./query-string-overview.md) as [PyObjects](./pyobjects.md). Convert one with Python's `int`, `float`, or `bool` before you use it, or [cast](./casting.md#python-variables) it in the [query string](./query-string-overview.md), for example `"A = (long)x"`.

The following example compares a raw NumPy scalar with one converted by Python's `int`:

```python order=source_np,source_np_meta
from deephaven import empty_table
import numpy as np

x = np.int64(5)
x_int = int(x)

source_np = empty_table(1).update(["Raw = x", "Converted = x_int"])
source_np_meta = source_np.meta_table
```

`Raw` is an [`org.jpy.PyObject`](./pyobjects.md) column, and `Converted` is a `byte` column.

Two [NumPy](./use-numpy.md) types are exceptions. `numpy.float64` is a subclass of Python's `float`, so it becomes a `double`. `numpy.str_` is a subclass of `str`, so it becomes a [`java.lang.String`](../reference/query-language/types/strings.md).

## Strings

As with [scalars](#scalars), a [query string](./query-string-overview.md) refers to a Python string variable by its name. The following example creates a table with two string columns:

```python order=source,source_meta
from deephaven import empty_table

my_first_string = "Hello, world!"
my_second_string = "Coding is fun."

source = empty_table(1).update(
    ["FirstString = my_first_string", "SecondString = my_second_string"]
)
source_meta = source.meta_table
```

Both columns are [`java.lang.String`](../reference/query-language/types/strings.md) columns.

## Sequences

In Python, sequences include lists, tuples, [NumPy arrays](./use-numpy.md), and more. Of these, the engine wraps only a list as a Java `List`. Tuples and [NumPy arrays](./use-numpy.md) reach the [query string](./query-string-overview.md) as [PyObjects](./pyobjects.md) unless you convert them to [Java arrays](./work-with-arrays.md#java-array-columns) first, as the end of this section shows.

A list works in a [query string](./query-string-overview.md) as is:

```python order=source_list,source_list_meta
from deephaven import empty_table

my_list = [1, 2, 3]

source_list = empty_table(1).update("ListColumn = my_list")
source_list_meta = source_list.meta_table
```

`source_list_meta` shows that `ListColumn` is an [`org.jpy.PyListWrapper`](./work-with-arrays.md#pylistwrapper-columns) column. [jpy](./use-jpy.md) uses this Java class to wrap a Python list. See [PyListWrapper columns](./work-with-arrays.md#pylistwrapper-columns) for the limitations of this column type.

You can also extract elements of a Python list in a [query string](./query-string-overview.md). The Deephaven engine can't infer the data type of an extracted list element, so the following example uses [type casts](./casting.md#type-casts) to set each column's type:

```python order=source,source_meta
from deephaven import empty_table

my_list = [1, 2, 3]

source = empty_table(1).update(
    [
        "FirstElement = (int)my_list[0]",
        "SecondElement = (int)my_list[1]",
        "ThirdElement = (int)my_list[2]",
    ]
)
source_meta = source.meta_table
```

With the [casts](./casting.md#type-casts), all three columns are `int` columns.

A [Java array](./work-with-arrays.md#java-array-columns) avoids calls into Python for each element, so it performs better than a list or a [PyObject](./pyobjects.md). [Use jpy](./use-jpy.md) to convert a Python sequence to a [Java primitive array](./work-with-arrays.md#java-array-columns) before you use it in a [query string](./query-string-overview.md). The following example converts a list and reads its first element:

```python order=source_java_array,source_java_array_meta
from deephaven import empty_table
import jpy

my_list = [1, 2, 3]
j_my_list = jpy.array("int", my_list)

source_java_array = empty_table(1).update(
    ["ArrayColumn = j_my_list", "FirstElement = j_my_list[0]"]
)
source_java_array_meta = source_java_array.meta_table
```

`source_java_array_meta` shows that `ArrayColumn` is an `int[]` column. Indexing a [Java array](./work-with-arrays.md#java-array-columns) keeps its element type, so `FirstElement` is an `int` column with no [cast](./casting.md#type-casts).

The [`deephaven.dtypes.array`](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.array) function also creates [Java arrays](./work-with-arrays.md#java-array-columns) from Python sequences, including tuples and [NumPy arrays](./use-numpy.md). See [Arrays](./work-with-arrays.md) for examples.

## Dicts

A Python dict reaches the [query string](./query-string-overview.md) as an `org.jpy.PyDictWrapper`, a Java `Map`. Look up a value with ``my_dict.get(`a`)`` or ``my_dict[`a`]``. The result is a [PyObject](./pyobjects.md), so [cast](./casting.md#python-variables) it to get a typed column:

```python order=source_dict,source_dict_meta
from deephaven import empty_table

my_dict = {"a": 1}

source_dict = empty_table(1).update(
    [
        "DictColumn = my_dict",
        "RawValue = my_dict.get(`a`)",
        "IntValue = (int)my_dict.get(`a`)",
    ]
)
source_dict_meta = source_dict.meta_table
```

`DictColumn` is an `org.jpy.PyDictWrapper` column, and `RawValue` is an [`org.jpy.PyObject`](./pyobjects.md) column. With the [cast](./casting.md#type-casts), `IntValue` is an `int` column.

## Related documentation

- [The Python-Java boundary and how it affects query efficiency](../conceptual/python-java-boundary.md)
- [An overview of query strings](./query-string-overview.md)
- [Query scope](./query-scope.md)
- [Python functions in query strings](./python-functions.md)
- [Python classes and objects in query strings](./python-classes.md)
- [Handle PyObjects in tables](./pyobjects.md)
- [Casting](./casting.md)
- [Arrays](./work-with-arrays.md)
- [Use jpy](./use-jpy.md)
- [Use NumPy in Deephaven queries](./use-numpy.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`meta_table`](../reference/table-operations/metadata/meta_table.md)
- [Nulls](../reference/query-language/types/nulls.md)
- [Strings](../reference/query-language/types/strings.md)
- [Special variables and constants](../reference/query-language/variables/special-variables.md)
- [`deephaven.dtypes.array` Pydoc](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.array)
- [`jpy` Pydoc](/core/pydoc/code/jpy.html)
