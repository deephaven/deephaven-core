---
title: Arrays
sidebar_label: Arrays
---

This guide shows you how to work with [arrays](../reference/query-language/types/arrays.md) in [query strings](./query-string-overview.md).

[Arrays](../reference/query-language/types/arrays.md) are an invaluable tool for grouping related data. They provide an easy way to access previous and future values in time series datasets. Support for [arrays](../reference/query-language/types/arrays.md) is built into the Deephaven Query Language (DQL).

## Array column types

Array columns fall into one of three categories of data type.

### Array columns

Array columns are Java arrays of primitive types. For example, the following query creates a table with a single row containing an array of primitive integers.

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(1).update("X = new int[]{1, 2, 3}")
source_meta = source.meta_table
```

You can also use [Python functions](./python-functions.md) to create Java primitive array columns:

```python order=source,source_meta
from deephaven import empty_table
from typing import Sequence
from numpy import typing as npt
import numpy as np


def list_func() -> Sequence[int]:
    return [4, 5, 6]


def np_array_func() -> npt.NDArray[np.intc]:
    return np.array([4, 5, 6])


source = empty_table(1).update(
    ["ArrayFromPython = list_func()", "ArrayFromNumpy = np_array_func()"]
)
source_meta = source.meta_table
```

The Deephaven engine can seamlessly work with these column types.

### Vector columns

Vector columns arise from common table operations including [grouping](./grouping-data.md). These vector columns are used in [dedicated aggregations](./dedicated-aggregations.md), [combined aggregations](./combined-aggregations.md), [`update_by`](../reference/table-operations/update-by-operations/updateBy.md), and more. The following example creates a vector column by calling [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md):

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(5).update(["X = ii % 2", "Y = ii"]).group_by("X")
source_meta = source.meta_table
```

### PyListWrapper columns

An `org.jpy.PyListWrapper` is [jpy's](./use-jpy.md) implementation of an array of Python objects. It is a generic object for an arbitrary Python sequence (list, NumPy array, tuple, etc.) whose type cannot be accurately inferred. It is less fully featured than the other array types.

This column type arises when Python sequences are used in query strings without explicit type information. For example:

```python order=source,source_meta
from deephaven import empty_table

my_list = [1, 2, 3]

source = empty_table(1).update("List = my_list")
source_meta = source.meta_table
```

`source_meta` shows that the resultant column type is [`org.jpy.PyListWrapper`](./pyobjects.md). This is fine in certain cases. However, this column type is the array equivalent of an [`org.jpy.PyObject`](./pyobjects.md) data type, which is not performant and does not support many basic operations. For this reason, it's recommended to convert Python objects to their Java equivalents using either jpy or the Deephaven array method.

**Using jpy:**

```python order=source_java_array_jpy,source_java_array_jpy_meta
from deephaven import empty_table
import jpy

my_list = [1, 2, 3]
j_my_list = jpy.array("int", my_list)

source_java_array_jpy = empty_table(1).update("ArrayColumn = j_my_list")
source_java_array_jpy_meta = source_java_array_jpy.meta_table
```

**Using deephaven.dtypes.array:**

```python order=source_java_array_dh,source_java_array_dh_meta
from deephaven import empty_table
from deephaven.dtypes import array, int32

my_list = [1, 2, 3]
dh_my_array = array(int32, my_list)

source_java_array_dh = empty_table(1).update("ArrayColumn = dh_my_array")
source_java_array_dh_meta = source_java_array_dh.meta_table
```

You can access individual elements from Python lists in [query strings](./query-string-overview.md) using standard indexing syntax. However, since Deephaven can't automatically determine the [data type](./data-types.md) of these elements, you must use explicit [type casts](./casting.md) to ensure proper types:

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

## Convert between arrays and vectors

Since Deephaven tables commonly use both Java primitive arrays and Deephaven vectors, it's useful to convert between the two. The following example converts between both vector and array columns:

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

## Create array columns

### By grouping

[Arrays](../reference/query-language/types/arrays.md) can be created using the [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) method to group data.

```python order=result,source
from deephaven import empty_table

source = empty_table(10).update(formulas=["X = (ii % 2 == 0) ? `A` : `B` ", "Y = ii"])

result = source.group_by(by=["X"])
```

### With aggregations

Certain aggregations create array columns. For example, the following operations create array columns:

- [`rolling_group_tick`](../reference/table-operations/update-by-operations/rolling-group-tick.md)
- [`rolling_group_time`](../reference/table-operations/update-by-operations/rolling-group-time.md)
- [`range_join`](../reference/table-operations/join/range-join.md)

The following example calls [`rolling_group_tick`](../reference/table-operations/update-by-operations/rolling-group-tick.md) to create an array column:

```python order=result,result_meta,source
from deephaven.updateby import rolling_group_tick
from deephaven import empty_table

source = empty_table(10).update(
    ["Key = (ii % 2 == 0) ? `A` : `B`", "Value = randomDouble(0, 1)"]
)

result = source.update_by(rolling_group_tick("TickGroup=Value", rev_ticks=3), "Key")
result_meta = result.meta_table
```

### Using the underscore operator

Every column in a table has an associated [array](../reference/query-language/types/arrays.md) variable, which can be accessed using the underscore (`_`) operator. This operator is specific to Deephaven. For example, a column called `X` can be accessed as an [array](../reference/query-language/types/arrays.md) by using the column name `X_`:

```python order=source,result
from deephaven import empty_table

source = empty_table(10).update(formulas=["X = ii"])
result = source.update(formulas=["A = X_"])
```

## Get array length

The [`len`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#len(byte[])) method returns the length of the given input. This is useful in query strings where you need to get the size of a [`Vector`](https://docs.deephaven.io/core/javadoc/io/deephaven/vector/Vector.html) or a Java array.

```python order=source,result
from deephaven import empty_table

source = empty_table(10).update(formulas=["X = i"]).group_by()
result = source.update("LenX = len(X)")
```

## Access array elements

The square bracket [operators](./operators.md) `[]` are used to access elements in [array](../reference/query-language/types/arrays.md) columns. The following example uses these operators to access the previous and next elements in the array, as well as print the size of the array:

```python order=source,result
from deephaven import empty_table

source = empty_table(10).update(formulas=["X = ii"])
result = source.update(formulas=["A = X_[ii - 1]", "B = X_[ii + 1]", "C = X_.size()"])
```

> [!NOTE]
> The first row of column `A` is null because there is no previous element at the zeroth array index. The last row of column `B` is null because there is no next element at the last array index.

Additionally, you can access specific [array](../reference/query-language/types/arrays.md) elements directly using indexes.

```python order=result,indexing_result
from deephaven import empty_table

result = (
    empty_table(10)
    .update(formulas=["X = (ii % 2 == 0) ? `A` : `B` ", "Y = ii"])
    .group_by(by=["X"])
)
indexing_result = result.update(formulas=["Element2 = Y[2]", "Element3 = Y[3]"])
```

## Slice arrays

You can slice [arrays](../reference/query-language/types/arrays.md) into subarrays with [`subVector`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/ssms/LongSegmentedSortedMultiset.html#subVector(long,long)). The first input is the index at which the slice starts, while the second is the index at which the slice ends. They are inclusive and exclusive, respectively.

The following example slices the [array](../reference/query-language/types/arrays.md) column, then [grabs specific elements](#access-array-elements) from the subarray.

```python order=result,result_meta,source
from deephaven import empty_table

source = (
    empty_table(10)
    .update(formulas=["X = (ii % 2 == 0) ? `A` : `B` ", "Y = ii"])
    .group_by(by=["X"])
)
result = source.update(
    formulas=[
        "SubArray = Y.subVector(2, 4)",
        "SubSlice = SubArray[1]",
    ]
)
result_meta = result.meta_table
```

## Functions with array arguments

### Built-in query language functions

> [!CAUTION]
> [Dedicated aggregations](./dedicated-aggregations.md), [Combined aggregations](./combined-aggregations.md), and [`update_by`](./rolling-aggregations.md) are always more performant than [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md) followed by manual calculations when used on ticking tables.

Many [built-in query language methods](./built-in-functions.md) take arrays as input. The following example uses the [`sum`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sum(io.deephaven.vector.LongVector)) and [`avg`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#avg(io.deephaven.vector.LongVector)) functions on a column containing [arrays](../reference/query-language/types/arrays.md).

```python order=result,sum_result
from deephaven import empty_table

result = (
    empty_table(10)
    .update(formulas=["X = (ii % 2 == 0) ? `A` : `B` ", "Y = ii"])
    .group_by(by=["X"])
)
sum_result = result.update(formulas=["ArraySum = sum(Y)", "ArrayAvg = avg(Y)"])
```

### Python functions

Python functions that take arrays as input are also supported in query strings. The following example calls a Python function that takes a [NumPy array](https://numpy.org/doc/stable/reference/generated/numpy.array.html) as input:

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

- [Create an empty table](./new-and-empty-table.md#empty_table)
- [Dedicated aggregations](./dedicated-aggregations.md)
- [Combined aggregations](./combined-aggregations.md)
- [Rolling aggregations](./rolling-aggregations.md)
- [Query string overview](./query-string-overview.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [Java objects in query strings](./java-classes.md)
- [Special variables](../reference/query-language/variables/special-variables.md)
- [Arrays](../reference/query-language/types/arrays.md)
- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`update`](../reference/table-operations/select/update.md)
