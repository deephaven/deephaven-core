---
title: Arrays vs vectors
sidebar_label: Vectors
---

Deephaven tables can store multi-element data in two forms: **Java primitive arrays** and **Deephaven vectors**. While both represent ordered collections of elements, they have important differences that affect how you work with them.

## Quick comparison

| Feature                  | Java Arrays                       | Deephaven Vectors                           |
| ------------------------ | --------------------------------- | ------------------------------------------- |
| **Type examples**        | `int[]`, `double[]`, `String[]`   | `IntVector`, `DoubleVector`, `ObjectVector` |
| **Index type**           | `int` (max ~2.1 billion elements) | `long` (supports larger datasets)           |
| **Out-of-bounds access** | Throws exception                  | Returns null constant (e.g., `NULL_INT`)    |
| **Created by**           | Explicit construction             | Table operations (`groupBy`, rolling ops)   |
| **Memory model**         | Owns contiguous memory            | May be a view into table data               |
| **Slicing**              | `Arrays.copyOfRange`              | `subVector` (no copy)                       |

## Java primitive arrays

Java arrays are standard fixed-size collections. Create them explicitly using Java syntax:

```python order=result,result_meta
from deephaven import empty_table

result = empty_table(1).update(
    [
        "IntArray = new int[]{1, 2, 3}",
        "DoubleArray = new double[]{1.1, 2.2, 3.3}",
        "StringArray = new String[]{`a`, `b`, `c`}",
    ]
)
result_meta = result.meta_table
```

Java arrays use `int`-based indexing. Accessing an index outside the array bounds throws an `ArrayIndexOutOfBoundsException`.

## Deephaven vectors

Deephaven vectors are Deephaven's specialized collection type, implemented by the [`Vector`](https://deephaven.io/core/javadoc/io/deephaven/vector/Vector.html) interface and its subclasses (`IntVector`, `DoubleVector`, etc.). Table operations that group data produce vector columns:

```python order=result,result_meta
from deephaven import empty_table

result = empty_table(5).update(["X = ii % 2", "Y = (int) ii"]).group_by("X")
result_meta = result.meta_table
```

The `Y` column is an `IntVector`, not an `int[]`.

### Key advantages of vectors

**Long-based indexing**: Vectors use `long` indices, supporting datasets larger than 2.1 billion elements.

**Safe out-of-bounds access**: Accessing an invalid index returns the appropriate null constant rather than throwing an exception:

```python order=source,result
from deephaven import empty_table

source = empty_table(3).update("X = ii")

result = source.update(
    [
        "ValidAccess = X_[1]",
        "OutOfBounds = X_[100]",
    ]
)
```

**Efficient slicing**: The `subVector` method creates a view without copying data:

```python order=source,result
from deephaven import empty_table

source = empty_table(10).update("X = ii").group_by()

result = source.update(
    [
        "Slice = X.subVector(2, 5)",
        "FirstOfSlice = Slice[0]",
    ]
)
```

**Column access with underscore**: Access any column as a vector using the `_` suffix:

```python order=source,result
from deephaven import empty_table

source = empty_table(5).update("X = ii")

result = source.update(
    [
        "PrevValue = X_[ii - 1]",
        "NextValue = X_[ii + 1]",
    ]
)
```

## Converting between arrays and vectors

Use the built-in `array` and `vec` functions to convert between types:

```python order=source,converted,converted_meta
from deephaven import empty_table

source = empty_table(5).update("X = ii").group_by()

converted = source.update(
    [
        "AsArray = array(X)",
        "BackToVector = vec(AsArray)",
    ]
)
converted_meta = converted.meta_table
```

### Supported conversions

| `vec()` input | `array()` input |
| ------------- | --------------- |
| `byte[]`      | `ByteVector`    |
| `char[]`      | `CharVector`    |
| `double[]`    | `DoubleVector`  |
| `float[]`     | `FloatVector`   |
| `int[]`       | `IntVector`     |
| `long[]`      | `LongVector`    |
| `short[]`     | `ShortVector`   |

## Choosing between arrays and vectors

**Use Java arrays when:**

- Passing data to external Java libraries that expect arrays
- Working with fixed, known-size data
- You need to mutate elements in place.

**Use Deephaven vectors when:**

- Working with grouped table data.
- Accessing previous or future row values via `Column_[ii ± n]`.
- Slicing data without copying.
- Handling potentially large datasets.

## Operations that create vectors

The following table operations produce vector columns:

- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`rolling_group_tick`](../reference/table-operations/update-by-operations/rolling-group-tick.md)
- [`rolling_group_time`](../reference/table-operations/update-by-operations/rolling-group-time.md)
- [`range_join`](../reference/table-operations/join/range-join.md)

## Related documentation

- [Work with arrays](../how-to-guides/work-with-arrays.md)
- [Arrays reference](../reference/query-language/types/arrays.md)
- [Convert array types](../reference/community-questions/convert-array-types.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [Vector Javadoc](https://deephaven.io/core/javadoc/io/deephaven/vector/Vector.html)
