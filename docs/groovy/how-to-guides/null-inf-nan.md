---
id: null-inf-nan
title: Null, infinity, and NaN values
sidebar_label: Nulls, infs, and NaNs
---

Null, infinity, and not-a-number (NaN) values can unexpectedly appear in your datasets and disrupt calculations if not handled properly. This guide shows you how to detect, filter, and replace these special values to keep your data analysis running smoothly. Not all numeric types support each of these special values.

In computing, null, infinity, and NaN are special values that represent missing, undefined, or indeterminate data. Nulls exist for all [Java primitive types](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html), while NaN and infinity exist only for floating-point types.

**Nulls** represent missing data. Each data type reserves a single, specific value to represent null. For example, Deephaven reserves `-32768` as the null value for the primitive `short` data type, resulting in an adjusted minimum value of `-32767` instead of the traditional `-32768`.

**NaNs** represent undefined mathematical results. For instance, the logarithm of a negative number is undefined, so that result is represented as NaN.

**Infinities** represent mathematically infinite values. Unlike NaNs, infinities are well-defined mathematically. For example, any positive number divided by `0` is undefined numerically but mathematically is defined as positive infinity.

## Available constants

Deephaven provides constant values representing null, infinity, and NaN values for each numeric type. These values are available in Groovy as well as [built into the query language](./built-in-constants.md).

### Nulls

The following null constants are available in Deephaven:

```groovy order=:log
nulls = [
    "boolean": NULL_BOOLEAN,
    "char": NULL_CHAR,
    "byte": NULL_BYTE,
    "short": NULL_SHORT,
    "int": NULL_INT,
    "long": NULL_LONG,
    "float": NULL_FLOAT,
    "double": NULL_DOUBLE
]

nulls.each { key, value ->
    println "Null value for ${key}: ${value}"
}
```

```groovy order=source,sourceMeta
source = emptyTable(1).update(
    "NullBoolean = NULL_BOOLEAN",
    "NullChar = NULL_CHAR",
    "NullByte = NULL_BYTE",
    "NullShort = NULL_SHORT",
    "NullInt = NULL_INT",
    "NullLong = NULL_LONG",
    "NullFloat = NULL_FLOAT",
    "NullDouble = NULL_DOUBLE",
)
sourceMeta = source.meta()
```

### NaNs

The following NaN constants are available in Deephaven:

```groovy order=:log
nanValues = [
    "float": NAN_FLOAT,
    "double": NAN_DOUBLE
]

nanValues.each { key, value ->
    println "NaN value for ${key}: ${value}"
}
```

```groovy order=source,sourceMeta
source = emptyTable(1).update(
    "NanFloat = NAN_FLOAT",
    "NanDouble = NAN_DOUBLE",
)
sourceMeta = source.meta()
```

### Infinities

The following infinity constants are available in Deephaven:

```groovy order=:log
infinityValues = [
    "pos_float": POS_INFINITY_FLOAT,
    "pos_double": POS_INFINITY_DOUBLE,
    "neg_float": NEG_INFINITY_FLOAT,
    "neg_double": NEG_INFINITY_DOUBLE
]

infinityValues.each { key, value ->
    println "Infinity value for ${key}: ${value}"
}
```

```groovy order=source,sourceMeta
source = emptyTable(1).update(
    "PosInfinityFloat = POS_INFINITY_FLOAT",
    "PosInfinityDouble = POS_INFINITY_DOUBLE",
    "NegInfinityFloat = NEG_INFINITY_FLOAT",
    "NegInfinityDouble = NEG_INFINITY_DOUBLE",
)
sourceMeta = source.meta()
```

### Built-in vs Python constants FIXME: XXX idk if this is true

As shown in the code blocks [above](#available-constants), the same constants are made available in both the Python API as well as the query language. As with all other operations done in [query strings](./query-string-overview.md), the built-in constants are the recommended way to use these constants. The Python API constants are provided for convenience and times where the constants are needed in Python code.

## Use null, infinity, and NaN values

Nulls, NaNs, and infinities are handled in similar ways in table operations.

### Built-in functions

The [functions built into the query language](./built-in-functions.md) gracefully handle null, NaN, and infinity values. For instance, the built-in [`sin`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) function handles them with no additional steps:

```groovy order=source
source = emptyTable(40).update(
    "X1 = (ii % 4 == 0) ? NULL_DOUBLE : 0.1 * ii",
    "Y1 = sin(X1)",
    "X2 = (ii % 4 == 0) ? NAN_DOUBLE : 0.1 * ii",
    "Y2 = sin(X2)",
    "X3 = (ii % 4 == 0) ? POS_INFINITY_DOUBLE : 0.1 * ii",
    "Y3 = sin(X3)",
    "X4 = (ii % 4 == 0) ? NEG_INFINITY_DOUBLE : 0.1 * ii",
    "Y4 = sin(X4)",
)
```

### Replace values

Replacing null, NaN, and infinity values is a common practice when a dataset should have a default value. The following code block replaces null values with `-1` using [built-in methods](./built-in-functions.md):

```groovy order=source
source = emptyTable(10).update(
    "HasNulls = (ii % 3 == 0) ? NULL_DOUBLE : randomDouble(0, 1)",
    "HasNaNs = (ii % 3 == 0) ? NAN_DOUBLE : randomDouble(0, 1)",
    "HasPosInfs = (ii % 3 == 0) ? POS_INFINITY_DOUBLE : randomDouble(0, 1)",
    "HasNegInfs = (ii % 3 == 0) ? NEG_INFINITY_DOUBLE : randomDouble(0, 1)",
    "ReplacedNulls = replaceIfNull(HasNulls, -1)",
    "ReplacedNaNs = replaceIfNaN(HasNaNs, -1)",
    "ReplacedPosInfs = replaceIfNonFinite(HasPosInfs, -1)",
    "ReplacedNegInfs = replaceIfNonFinite(HasNegInfs, -1)",
)
```

### Remove values

[Built-in methods](./built-in-functions.md) can also be used to [filter](./filters.md) out null and NaN values. There is no built-in method specifically to remove infinity values.

```groovy order=source,resultNoNulls,resultNoNans,resultNoNullsNans
source = emptyTable(10).update(
    "HasNulls = (ii % 3 == 0) ? NULL_DOUBLE : randomDouble(0, 1)",
    "HasNaNs = (ii % 4 == 2) ? NAN_DOUBLE : randomDouble(5, 10)",
)

resultNoNulls = source.where("!isNull(HasNulls)")
resultNoNans = source.where("!isNaN(HasNaNs)")
resultNoNullsNans = source.where("!isNull(HasNulls)", "!isNaN(HasNaNs)")
```

### User-defined functions FIXME: idk about this part yet

When passing null, NaN, or infinity values from tables into Python functions, the [`deephaven.constants`](https://docs.deephaven.io/core/pydoc/code/deephaven.constants.html) module is useful. You can check input values as follows:

```python order=source,result
from deephaven import empty_table
from deephaven.constants import (
    NULL_DOUBLE,
    NAN_DOUBLE,
    POS_INFINITY_DOUBLE,
    NEG_INFINITY_DOUBLE,
)


def my_func(input_value) -> float:
    if input_value == NULL_DOUBLE:
        return -1
    elif input_value == NAN_DOUBLE:
        return -2
    elif input_value == POS_INFINITY_DOUBLE:
        return -3
    else:
        return input_value * 2


source = empty_table(10).update(
    [
        "HasNulls = (ii % 3 == 0) ? NULL_DOUBLE : randomDouble(0, 1)",
        "HasNaNs = (ii % 4 == 2) ? NAN_DOUBLE : randomDouble(5, 10)",
        "HasPosInfs = (ii % 5 == 4) ? POS_INFINITY_DOUBLE : randomDouble(10, 15)",
        "HasNegInfs = (ii % 6 == 5) ? NEG_INFINITY_DOUBLE : randomDouble(15, 20)",
    ]
)

result = source.update(
    [
        "ResultNulls = my_func(HasNulls)",
        "ResultNaNs = my_func(HasNaNs)",
        "ResultPosInfs = my_func(HasPosInfs)",
        "ResultNegInfs = my_func(HasNegInfs)",
    ]
)
```

## Related documentation

- [Create an empty table](./new-and-empty-table.md#emptytable)
- [Create a new table](./new-and-empty-table.md#newtable)
- [Query string overview](./query-string-overview.md)
- [Filters in query strings](./filters.md)
- [Formulas in query strings](./formulas.md)
- [Operators in query strings](./operators.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Groovy closures in query strings](./groovy-closures.md)
- [Auto-imported functions](../reference/query-language/query-library/auto-imported-functions.md)
- [Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.constants.html) FIXME: XXX
