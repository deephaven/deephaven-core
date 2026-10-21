---
title: Arrays
---

This guide shows you how to work with [arrays](../reference/query-language/types/arrays.md) in [query strings](./query-string-overview.md).

[Arrays](../reference/query-language/types/arrays.md) are an invaluable tool for grouping related data. They provide an easy way to access previous and future values in time-series data, and support for them is built into the Deephaven Query Language (DQL).

## Array column types

On this page, "array column" means a column of [Java arrays](#java-array-columns), [Deephaven vectors](#vector-columns), or [Python lists wrapped as `PyListWrapper`](#pylistwrapper-columns). The following sections describe each. "Java array" always means the first kind.

### Java array columns

Java array columns hold Java arrays, most often of primitive types such as `int[]` or `double[]`. For example, the following query creates a table with a single row containing an array of primitive integers.

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(1).update("X = new int[]{1, 2, 3}")
source_meta = source.meta_table
```

You can also create Java primitive array columns with [Python functions](./python-functions.md). When a function has a sequence or NumPy array [return type hint](./python-functions.md#array-return-values), as `list_func` and `np_array_func` do, Deephaven stores the result as a Java array:

```python order=source,source_meta
from deephaven import empty_table
from collections.abc import Sequence
import numpy.typing as npt
import numpy as np


def list_func() -> Sequence[int]:
    return [4, 5, 6]


def np_array_func() -> npt.NDArray[np.int32]:
    return np.array([4, 5, 6], dtype=np.int32)


source = empty_table(1).update(
    ["ArrayFromPython = list_func()", "ArrayFromNumpy = np_array_func()"]
)
source_meta = source.meta_table
```

`ArrayFromPython` is a `long[]` column, because Deephaven maps Python `int` to Java `long`. `ArrayFromNumpy` is an `int[]` column, because of the `np.int32` hint.

[Built-in query language functions](./built-in-functions.md) such as [`sum`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html) and [`len`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#len(byte[])) accept Java array columns directly. For example, `len(X)` returns 3 for the `X` column in the first example. The same functions work on [vector columns](#vector-columns), as [Get array length](#get-array-length) and [Functions with array arguments](#functions-with-array-arguments) show.

### Vector columns

Vector columns hold Deephaven [vectors](https://deephaven.io/core/javadoc/io/deephaven/vector/Vector.html), such as [`LongVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/LongVector.html) or [`DoubleVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/DoubleVector.html), rather than Java arrays. Operations such as [grouping](./grouping-data.md) produce them. The following example creates a vector column by calling [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md):

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(5).update(["X = ii % 2", "Y = ii"]).group_by("X")
source_meta = source.meta_table
```

For other operations that produce vector columns, see [Create vector columns](#create-vector-columns). To compare vectors with Java arrays, see [Arrays vs vectors](../conceptual/arrays-vs-vectors.md).

### PyListWrapper columns

An `org.jpy.PyListWrapper` is [jpy's](./use-jpy.md) Java `List` wrapper around a Python list. Its elements are Python objects, so Deephaven can't infer their type.

This column type appears when you use a Python list in a [query string](./query-string-overview.md) without explicit type information, as in the following example:

```python order=source,source_meta
from deephaven import empty_table

my_list = [1, 2, 3]

source = empty_table(1).update("List = my_list")
source_meta = source.meta_table
```

`source_meta` shows that the `List` column has the type `org.jpy.PyListWrapper`. Only Python lists become `PyListWrapper` columns. Other Python values reach the [query string](./query-string-overview.md) as follows:

- Tuples, NumPy arrays, and other sequences that aren't strings become [PyObjects](./pyobjects.md).
- Strings become Java [`String`](../reference/query-language/types/strings.md) values.
- Java arrays, such as those that the [jpy](./use-jpy.md) and [`deephaven.dtypes.array`](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.array) examples below create, stay Java arrays.

You can access individual elements from Python lists in [query strings](./query-string-overview.md) using standard indexing syntax. However, Deephaven can't infer the [data type](./data-types.md) of these elements, so [cast](./casting.md) each one to the type you need:

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

`PyListWrapper` is the array equivalent of [`org.jpy.PyObject`](./pyobjects.md). [Query strings](./query-string-overview.md) process it slowly, and fewer operations accept it than the other array column types. To avoid it, convert the list to a Java array with [jpy](./use-jpy.md) or [`deephaven.dtypes.array`](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.array) before you use it in a [query string](./query-string-overview.md).

**Using [jpy](./use-jpy.md):**

```python order=source_java_array_jpy,source_java_array_jpy_meta
from deephaven import empty_table
import jpy

my_list = [1, 2, 3]
j_my_list = jpy.array("int", my_list)

source_java_array_jpy = empty_table(1).update("ArrayColumn = j_my_list")
source_java_array_jpy_meta = source_java_array_jpy.meta_table
```

**Using [`deephaven.dtypes.array`](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.array):**

```python order=source_java_array_dh,source_java_array_dh_meta
from deephaven import empty_table
from deephaven.dtypes import array, int32

my_list = [1, 2, 3]
dh_my_array = array(int32, my_list)

source_java_array_dh = empty_table(1).update("ArrayColumn = dh_my_array")
source_java_array_dh_meta = source_java_array_dh.meta_table
```

## Create vector columns

Grouping with [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md), shown in [Vector columns](#vector-columns), is one way to create vector columns. This section shows two others. [Several operations](#using-aggregations-update_by-and-joins) collect values into vectors, and the [underscore operator](#using-the-underscore-operator) gives a vector view of any column.

### Using aggregations, `update_by`, and joins

Several operations produce vector columns:

- [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md)
- [`rolling_group_tick`](../reference/table-operations/update-by-operations/rolling-group-tick.md)
- [`rolling_group_time`](../reference/table-operations/update-by-operations/rolling-group-time.md)
- [`range_join`](../reference/table-operations/join/range-join.md)

The following example calls [`rolling_group_tick`](../reference/table-operations/update-by-operations/rolling-group-tick.md) to create a vector column:

```python order=result,result_meta,source
from deephaven.updateby import rolling_group_tick
from deephaven import empty_table

source = empty_table(10).update(
    ["Key = (ii % 2 == 0) ? `A` : `B`", "Value = randomDouble(0, 1)"]
)

result = source.update_by(rolling_group_tick("TickGroup = Value", rev_ticks=3), "Key")
result_meta = result.meta_table
```

Each `TickGroup` cell holds the current row's `Value` and up to two earlier `Value`s with the same `Key`, because the `rev_ticks=3` window includes the current row.

### Using the underscore operator

Every column in a table has an associated column array variable, which holds the column's values as a vector. You refer to it by adding the underscore (`_`) operator to the column name. This syntax is specific to Deephaven. For example, `X_` is the column array variable for column `X`:

```python order=source,result
from deephaven import empty_table

source = empty_table(10).update(formulas=["X = ii"])
result = source.update(formulas=["A = X_"])
```

Each row of `A` holds the entire `X` column as a vector.

> [!NOTE]
> By default, the engine rejects formulas that use column array variables such as `X_` on [refreshing tables](../conceptual/table-update-model.md), because it can't safely update their results as the table changes. The engine allows two kinds of these formulas anyway:
>
> - Formulas on [blink tables](../conceptual/table-types.md#specialization-3-blink).
> - Formulas on any table that read a constant offset from the current row, such as `X_[ii - 1]`. Here, [`ii`](../reference/query-language/variables/special-variables.md) is the current row index.
>
> See [Refreshing table restrictions](../reference/query-language/variables/special-variables.md#refreshing-table-restrictions) for the full rules and [Alternatives for refreshing tables](./built-in-variables.md#alternatives-for-refreshing-tables) for other approaches.

## Convert between Java arrays and vectors

Since Deephaven tables commonly use both Java primitive arrays and Deephaven vectors, it's useful to convert between the two. Use the built-in [`array`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#array(io.deephaven.vector.LongVector)) function to convert a vector to a Java array, and [`vec`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#vec(long...)) to convert a Java array to a vector. The following example converts a vector column to an array column, then converts that array column back to a vector column:

```python order=result,result_meta,source
from deephaven import empty_table

source = empty_table(10).update("Vector = ii").group_by()
result = source.update(
    [
        "ArrayFromVector = array(Vector)",
        "VectorFromArray = vec(ArrayFromVector)",
    ]
)
result_meta = result.meta_table
```

To convert a Python list to a Java array instead, see [PyListWrapper columns](#pylistwrapper-columns).

## Get array length

The [`len`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#len(byte[])) function returns the length of its argument. Use it in a [query string](./query-string-overview.md) to get the size of a [`Vector`](https://deephaven.io/core/javadoc/io/deephaven/vector/Vector.html) or a Java array.

```python order=source,result
from deephaven import empty_table

source = empty_table(10).update(formulas=["X = i"]).group_by()
result = source.update("LenX = len(X)")
```

Vectors also have a [`size`](https://deephaven.io/core/javadoc/io/deephaven/util/datastructures/LongSizedDataStructure.html#size()) method, so `X.size()` returns the same value as `len(X)` in this example.

## Access array elements

Use the square bracket [operators](./operators.md) `[]` to access elements in [array](../reference/query-language/types/arrays.md) columns. The following example indexes `X_`, the column array variable for column `X` described in [Using the underscore operator](#using-the-underscore-operator). It uses the row index [`ii`](../reference/query-language/variables/special-variables.md) to read the elements before and after the current row:

```python order=source,result
from deephaven import empty_table

source = empty_table(10).update(formulas=["X = ii"])
result = source.update(formulas=["A = X_[ii - 1]", "B = X_[ii + 1]"])
```

> [!NOTE]
> The first row of column `A` is null because `X_[ii - 1]` reads index -1, which is out of bounds. The last row of column `B` is null because `X_[ii + 1]` reads index 10, one past the last element.
>
> On a vector, any out-of-range index returns null, even a negative one. On a Java array, an out-of-range index throws an `ArrayIndexOutOfBoundsException`, so check the index against [`len`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#len(byte[])) first.

To read the same position from every row's [array](../reference/query-language/types/arrays.md), use a constant index. The following example reads the elements at indexes 2 and 3 of each group in the vector column `Y`:

```python order=result,indexing_result
from deephaven import empty_table

result = (
    empty_table(10)
    .update(formulas=["X = (ii % 2 == 0) ? `A` : `B`", "Y = ii"])
    .group_by(by=["X"])
)
indexing_result = result.update(formulas=["Element2 = Y[2]", "Element3 = Y[3]"])
```

## Slice arrays

You can slice a vector into a smaller vector with [`subVector`](https://deephaven.io/core/javadoc/io/deephaven/vector/Vector.html#subVector(long,long)). The first argument is the start index, which is inclusive, and the second is the end index, which is exclusive.

To slice a Java array, first convert it to a vector with [`vec`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#vec(long...)), as shown in [Convert between Java arrays and vectors](#convert-between-java-arrays-and-vectors).

The following example slices the vector column `Y`, then [grabs a specific element](#access-array-elements) from the slice.

```python order=result,result_meta,source
from deephaven import empty_table

source = (
    empty_table(10)
    .update(formulas=["X = (ii % 2 == 0) ? `A` : `B`", "Y = ii"])
    .group_by(by=["X"])
)
result = source.update(
    formulas=[
        "SubVector = Y.subVector(2, 4)",
        "SubVectorElement = SubVector[1]",
    ]
)
result_meta = result.meta_table
```

## Functions with array arguments

### Built-in query language functions

Many [built-in query language functions](./built-in-functions.md) take arrays as input. The following example uses the [`sum`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sum(io.deephaven.vector.LongVector)) and [`avg`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#avg(io.deephaven.vector.LongVector)) functions on a vector column created by [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md). The same functions also accept Java array columns.

```python order=result,sum_result
from deephaven import empty_table

result = (
    empty_table(10)
    .update(formulas=["X = (ii % 2 == 0) ? `A` : `B`", "Y = ii"])
    .group_by(by=["X"])
)
sum_result = result.update(formulas=["ArraySum = sum(Y)", "ArrayAvg = avg(Y)"])
```

> [!CAUTION]
> On [refreshing tables](../conceptual/table-update-model.md), which update as new data arrives, avoid [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) followed by calculations like the one above. Use [dedicated aggregations](./dedicated-aggregations.md), [combined aggregations](./combined-aggregations.md), or [rolling aggregations with `update_by`](./rolling-aggregations.md) instead. Their built-in statistics, such as sums and averages, update incrementally. A formula over a grouped column recomputes the whole group each time the group changes.

### Python functions

[Query strings](./query-string-overview.md) can also call [Python functions](./python-functions.md) that take arrays as input. The following example calls a [Python function](./python-functions.md) that takes a [NumPy array](https://numpy.org/doc/stable/reference/generated/numpy.array.html) as input:

```python order=source,source_meta
from deephaven import empty_table
import numpy as np
import numpy.typing as npt


def np_array_func(arr: npt.NDArray[np.float64]) -> np.double:
    return np.sum(arr)


source = empty_table(1).update(
    [
        "ArrayColumn = new double[]{3.14, 1.23, -0.919}",
        "CallNumpy = np_array_func(ArrayColumn)",
    ]
)
source_meta = source.meta_table
```

## Related documentation

- [Arrays vs vectors](../conceptual/arrays-vs-vectors.md)
- [Incremental update model](../conceptual/table-update-model.md)
- [Deephaven's table types](../conceptual/table-types.md)
- [Create an empty table](./new-and-empty-table.md#empty_table)
- [Dedicated aggregations](./dedicated-aggregations.md)
- [Combined aggregations](./combined-aggregations.md)
- [Rolling aggregations](./rolling-aggregations.md)
- [Query string overview](./query-string-overview.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Java objects in query strings](./java-classes.md)
- [Group and ungroup data](./grouping-data.md)
- [Use jpy](./use-jpy.md)
- [Handle PyObjects in tables](./pyobjects.md)
- [Data types in Deephaven and Python](./data-types.md)
- [Casting](./casting.md)
- [Operators](./operators.md)
- [Special variables](../reference/query-language/variables/special-variables.md)
- [Arrays](../reference/query-language/types/arrays.md)
- [Strings](../reference/query-language/types/strings.md)
- [`agg.group`](../reference/table-operations/group-and-aggregate/AggGroup.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`meta_table`](../reference/table-operations/metadata/meta_table.md)
- [`range_join`](../reference/table-operations/join/range-join.md)
- [`rolling_group_tick`](../reference/table-operations/update-by-operations/rolling-group-tick.md)
- [`rolling_group_time`](../reference/table-operations/update-by-operations/rolling-group-time.md)
- [`update`](../reference/table-operations/select/update.md)
- [`update_by`](../reference/table-operations/update-by-operations/updateBy.md)
- [`deephaven.dtypes.array` Pydoc](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.array)
- [`Vector` Javadoc](/core/javadoc/io/deephaven/vector/Vector.html)
- [`Basic` Javadoc](/core/javadoc/io/deephaven/function/Basic.html)
- [`Numeric` Javadoc](/core/javadoc/io/deephaven/function/Numeric.html)
