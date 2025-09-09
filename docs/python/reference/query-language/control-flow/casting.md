---
title: Casting
---

Casting is performed in [formulas](../../../how-to-guides/formulas.md) to convert one data type to another. It is used in the Deephaven Query Language (DQL) to ensure the correctness of column data types.

## Usage

### Scalars

For scalar values, `(type)` casting casts from one type handled by Java to another.

- byte
- short
- int
- long
- float
- double

> [!NOTE]
> Deephaven [column expressions](../../../how-to-guides/use-select-view-update.md) are [Java](https://docs.oracle.com/javase/specs/jls/se7/html/jls-5.html#jls-5.5) expressions with some extra features. The rules of casting and numbers are consistent with Java.

Each number type has an allotted number of bytes used to store information. Depending on your data needs, consider the data type used in your application.

| Type   | Bytes | Description                   | Example                                    | Example                                   |
| ------ | ----- | ----------------------------- | ------------------------------------------ | ----------------------------------------- |
| byte   | 1     | signed whole numbers          | -123                                       | 123                                       |
| short  | 2     | signed whole numbers          | -30,000                                    | 30,000                                    |
| int    | 4     | signed whole numbers          | -2,634,123                                 | 2,634,123                                 |
| long   | 8     | signed whole numbers          | -8,293,193,496                             | 8,293,193,496                             |
| float  | 4     | signed floating point numbers | -8,293,193,496.2948293                     | 8,293,193,496.2948293                     |
| double | 8     | signed floating point numbers | -64,123,542,927,328,293,193,496.2948293231 | 64,123,542,927,328,293,193,496.2948293231 |

### Strings

Any columns that contain scalar values or string values can be converted to a [String](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html). The prefix, `java.lang`, is not necessary.

### Lists and Arrays

Python and Java arrays share similarities, but are not equal. The query language has a much easier time dealing with Java arrays than it does with Python arrays (lists, NumPy arrays, etc.). Thus, when a Python method would return one of these types, it's typically worthwhile to cast the output to an array type better handled by Deephaven's query engine.

Deephaven's [dtypes](../../python/deephaven-python-types.md) module contains many utilities for converting Python arrays to Java arrays.

## Examples

### Widening conversion

When operations are applied on a type of number that widen the type, the casting will automatically change.

In the following example, column `A` is assigned an integer row element in the `source` table. When operations are applied to that number that require more precision than integer, type allows the new columns to be casted to doubles.

```python order=result,source
from deephaven import empty_table

source = empty_table(10).update(
    formulas=["A = (long)i", "B = A * sqrt(2)", "C = A / 2"]
)

result = source.meta_table
```

### Manually casting

When writing queries, one might need to narrow the casting of the number type. The following example takes a number and reduces the bytes used to store that information. Since the bytes are truncated when narrowing the casting, spurious numbers will result if the number requires more bytes to hold the data.

The table below shows the minimum and maximum values for each data type.

> [!CAUTION]
> The boundary point of each number type might be assigned unexpected values, such as [null](../types/nulls.md) or infinity. If the data is near these boundaries, use a type that allows for more storage.

```python order=numbers_max,numbers_min,numbers_min_meta,numbers_max_meta
from deephaven import new_table
from deephaven.column import double_col

numbers_max = new_table(
    [
        double_col(
            "MaxNumbers",
            [
                (2 - 1 / (2**52)) * (2**1023),
                (2 - 1 / (2**23)) * (2**127),
                (2**63) - 1,
                (2**31) - 1,
                (2**15) - 1,
                (2**7) - 1,
            ],
        )
    ]
).view(
    formulas=[
        "DoubleMax = (double)MaxNumbers",
        "FloatMax = (float)MaxNumbers",
        "LongMax = (long)MaxNumbers",
        "IntMax = (int)MaxNumbers",
        "ShortMax = (short)MaxNumbers",
        "ByteMax = (byte)MaxNumbers",
    ]
)

numbers_min = new_table(
    [
        double_col(
            "MinNumbers",
            [
                1 / (2**1074),
                1 / (2**149),
                -(2**63) + 513,
                -(2**31) + 2,
                -1 * (2**15) + 1,
                -(2**7) + 1,
            ],
        )
    ]
).view(
    formulas=[
        "DoubleMin = (double)MinNumbers",
        "FloatMin = (float)MinNumbers",
        "LongMin = (long)MinNumbers",
        "IntMin = (int)MinNumbers",
        "ShortMin = (short)MinNumbers ",
        "ByteMin = (byte)MinNumbers ",
    ]
)

numbers_min_meta = numbers_min.meta_table.view(formulas=["Name", "DataType"])
numbers_max_meta = numbers_max.meta_table.view(formulas=["Name", "DataType"])
```

### Casting strings

Sometimes, you must cast objects explicitly to a string type for `update` operations to read the query correctly.

```python order=source,result
from deephaven import empty_table
from deephaven import agg

colors = ["Red", "Blue", "Green"]

formulas = [
    "X = 0.1 * i",
    "Y1 = Math.pow(X, 2)",
    "Y2 = Math.sin(X)",
    "Y3 = Math.cos(X)",
]
grouping_cols = ["Letter = (i % 2 == 0) ? `A` : `B`", "Color = (String)colors[i % 3]"]

source = empty_table(40).update(formulas + grouping_cols)

myagg = [
    agg.formula(
        formula="avg(k)",
        formula_param="k",
        cols=[f"AvgY{idx} = Y{idx}" for idx in range(1, 4)],
    )
]

result = source.agg_by(aggs=myagg, by=["Letter", "Color"])
```

### Casting arrays

The code below uses a Python function to create a table with 5 rows and 1 column named `X`. `X` contains arrays with randomly generated double precision numbers between 0 and 1. Without any casting, the data type of the `X` column is `org.jpy.PyObject`. This data type is a result of the Deephaven engine being told nothing about the Python values returned. It's a safe choice for the engine. However, this data type is not usable for many table operations, including [ungrouping](../../table-operations/group-and-aggregate/ungroup.md).

```python order=source,source_meta
from deephaven import empty_table
import random


def create_list(length):
    return_arr = [None] * length
    for idx in range(length):
        return_arr[idx] = random.random()
    return return_arr


source = empty_table(5).update(["X = create_list(3)"])
source_meta = source.meta_table
```

Trying to cast the output of `create_list` to a Java double array directly will fail.

```python should-fail
from deephaven import empty_table
import random


def create_list(length):
    return_arr = [None] * length
    for idx in range(length):
        return_arr[idx] = random.random()
    return return_arr


source = empty_table(5).update(["X = (double[])create_list(3)"])
```

`deephaven.dtypes` can be used to cast the output of the function to a Java double array. This enables the double array cast `(double[])` in the query string.

```python order=source,source_meta
from deephaven import dtypes as dht
from deephaven import empty_table
import random


def create_list(length):
    return_arr = [None] * length
    for idx in range(length):
        return_arr[idx] = random.random()
    return dht.array(dtype=dht.double, seq=return_arr)


source = empty_table(5).update(["X = (double[])create_list(3)"])
source_meta = source.meta_table
```

The `source` table can now be ungrouped.

## Related documentation

- [Access metadata](../../../how-to-guides/metadata.md)
- [Built-in query language constants](../../../how-to-guides/built-in-constants.md)
- [Built-in query language variables](../../../how-to-guides/built-in-variables.md)
- [Built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#empty_table)
- [Filters in query strings](../../../how-to-guides/filters.md)
- [Formulas in query strings](../../../how-to-guides/formulas.md)
- [How to use filters](../../../how-to-guides/use-filters.md)
- [How to use select, view, and update](../../../how-to-guides/use-select-view-update.md)
- [Operators in query strings](../../../how-to-guides/operators.md)
- [Query language functions](../query-library/auto-imported-functions.md)
- [`update`](../../table-operations/select/update.md)
