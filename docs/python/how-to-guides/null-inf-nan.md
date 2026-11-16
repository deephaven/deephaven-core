---
title: Null, NaN, and infinity values
---

Null, not-a-number (NaN), and infinity values can unexpectedly appear in your datasets and disrupt calculations if you don't handle them. In a [real-time table](../conceptual/table-types.md), any update can bring in a new one, so it pays to handle them in the query itself.

This guide shows you how to keep your data analysis running smoothly. It lists the constants Deephaven provides for these special values, then shows how [built-in functions](./built-in-functions.md) treat them and how to replace them, [filter](./use-filters.md) them out, and check for them in your own [Python functions](./python-functions.md).

**Nulls** represent missing data. For a [primitive](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) numeric type or `char`, Deephaven reserves one value of the type as its null. For example, it reserves `-32768` as the null value for the primitive `short` data type, so the minimum `short` value is `-32767` instead of the traditional `-32768`. Booleans and object types use Java `null` instead.

**NaNs** represent undefined mathematical results. For instance, the logarithm of a negative number is undefined, so [`log`](/core/javadoc/io/deephaven/function/Numeric.html#log(double)) returns NaN for a negative input.

**Infinities** represent mathematically infinite values. For example, in floating-point arithmetic, dividing a positive number by `0.0` produces positive infinity.

## Available constants

Deephaven provides a null constant for each primitive numeric type and `char`, plus [`NULL_BOOLEAN`](./built-in-constants.md#null-constants), which equals Java `null`, for `Boolean` columns. Only `float` and `double` have NaN and infinity constants.

Each constant comes in two copies. Use the [built-in constants](./built-in-constants.md) in [query strings](./query-string-overview.md), and use the [`deephaven.constants`](/core/pydoc/code/deephaven.constants.html) module in Python code. In each subsection below, the first example prints the Python values, and the second uses the [built-in constants](./built-in-constants.md) in a [query string](./query-string-overview.md) and shows the column types with [`meta_table`](../reference/table-operations/metadata/meta_table.md).

### Nulls

The following null constants are available in Deephaven:

```python order=:log
from deephaven.constants import (
    NULL_BOOLEAN,
    NULL_CHAR,
    NULL_BYTE,
    NULL_SHORT,
    NULL_INT,
    NULL_LONG,
    NULL_FLOAT,
    NULL_DOUBLE,
)

nulls = {
    "boolean": NULL_BOOLEAN,
    "char": NULL_CHAR,
    "byte": NULL_BYTE,
    "short": NULL_SHORT,
    "int": NULL_INT,
    "long": NULL_LONG,
    "float": NULL_FLOAT,
    "double": NULL_DOUBLE,
}
for key in nulls:
    print(f"Null value for {key}: {nulls[key]}")
```

For `float` and `double`, the null value printed above is the most negative finite value of the type. The smallest non-null finite value is one step above it. For `double`, that value is [`MIN_FINITE_DOUBLE`](./built-in-constants.md#minimum-and-maximum-constants).

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(1).update(
    [
        "NullBoolean = NULL_BOOLEAN",
        "NullChar = NULL_CHAR",
        "NullByte = NULL_BYTE",
        "NullShort = NULL_SHORT",
        "NullInt = NULL_INT",
        "NullLong = NULL_LONG",
        "NullFloat = NULL_FLOAT",
        "NullDouble = NULL_DOUBLE",
    ]
)
source_meta = source.meta_table
```

### NaNs

The following NaN constants are available in Deephaven:

```python order=:log
from deephaven.constants import NAN_DOUBLE, NAN_FLOAT

nan_values = {
    "float": NAN_FLOAT,
    "double": NAN_DOUBLE,
}
for key in nan_values:
    print(f"NaN value for {key}: {nan_values[key]}")
```

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(1).update(
    [
        "NanFloat = NAN_FLOAT",
        "NanDouble = NAN_DOUBLE",
    ]
)
source_meta = source.meta_table
```

### Infinities

The following infinity constants are available in Deephaven:

```python order=:log
from deephaven.constants import (
    POS_INFINITY_DOUBLE,
    POS_INFINITY_FLOAT,
    NEG_INFINITY_DOUBLE,
    NEG_INFINITY_FLOAT,
)

infinity_values = {
    "pos_float": POS_INFINITY_FLOAT,
    "pos_double": POS_INFINITY_DOUBLE,
    "neg_float": NEG_INFINITY_FLOAT,
    "neg_double": NEG_INFINITY_DOUBLE,
}
for key in infinity_values:
    print(f"Infinity value for {key}: {infinity_values[key]}")
```

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(1).update(
    [
        "PosInfinityFloat = POS_INFINITY_FLOAT",
        "PosInfinityDouble = POS_INFINITY_DOUBLE",
        "NegInfinityFloat = NEG_INFINITY_FLOAT",
        "NegInfinityDouble = NEG_INFINITY_DOUBLE",
    ]
)
source_meta = source.meta_table
```

## Handle null, NaN, and infinity values

Each example in this section builds columns that hold special values in selected rows. To choose those rows, the examples use the [ternary conditional operator](./ternary-if-how-to.md) with [`ii`](../reference/query-language/variables/special-variables.md), the [built-in variable](./built-in-variables.md) that holds the row position.

### Built-in functions

The [functions built into the query language](./built-in-functions.md) accept null, NaN, and infinity values without extra checks in your query, but they treat each kind differently:

- **Null:** most functions skip null inputs or return null.
- **NaN and infinity:** functions follow IEEE floating-point rules, so a NaN or infinite input usually gives a NaN or infinite result.

For example, [`avg`](/core/javadoc/io/deephaven/function/Numeric.html#avg(double...)) skips null values but returns NaN for an array that contains a NaN.

The following example passes each kind of value to the built-in [`sin`](/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) function. Every fourth row holds the special value:

```python order=source
from deephaven import empty_table

source = empty_table(40).update(
    [
        "X1 = (ii % 4 == 0) ? NULL_DOUBLE : 0.1 * ii",
        "Y1 = sin(X1)",
        "X2 = (ii % 4 == 0) ? NAN_DOUBLE : 0.1 * ii",
        "Y2 = sin(X2)",
        "X3 = (ii % 4 == 0) ? POS_INFINITY_DOUBLE : 0.1 * ii",
        "Y3 = sin(X3)",
        "X4 = (ii % 4 == 0) ? NEG_INFINITY_DOUBLE : 0.1 * ii",
        "Y4 = sin(X4)",
    ]
)
```

Where the input is null, `Y1` is null. Where the input is NaN or infinite, `Y2`, `Y3`, and `Y4` are NaN.

Arithmetic [operators](./operators.md#arithmetic-operators) in [query strings](./query-string-overview.md) treat nulls the same way: if either operand is null, the result is null. To test for a null, use `X == NULL_DOUBLE`. To test for a NaN, use [`isNaN(X)`](/core/javadoc/io/deephaven/function/Numeric.html#isNaN(double)), because `X == NAN_DOUBLE` is never `true`. [Comparison operators](./operators.md#comparison-operators) covers the other null and NaN comparison rules.

### Replace values

Replacing null, NaN, and infinity values is a common practice when a dataset should have a default value. The following code block uses [built-in functions](./built-in-functions.md) to replace each kind of value with `-1`:

- [`replaceIfNull`](/core/javadoc/io/deephaven/function/Basic.html#replaceIfNull(double,double)) replaces null values.
- [`replaceIfNaN`](/core/javadoc/io/deephaven/function/Numeric.html#replaceIfNaN(double,double)) replaces NaN values.
- [`replaceIfNullNaN`](/core/javadoc/io/deephaven/function/Numeric.html#replaceIfNullNaN(double,double)) replaces null and NaN values, but not infinite values.
- [`replaceIfNonFinite`](/core/javadoc/io/deephaven/function/Numeric.html#replaceIfNonFinite(double,double)) replaces infinite values. It also replaces null and NaN values.

```python order=source
from deephaven import empty_table

source = empty_table(10).update(
    [
        "HasNulls = (ii % 3 == 0) ? NULL_DOUBLE : randomDouble(0, 1)",
        "HasNaNs = (ii % 3 == 0) ? NAN_DOUBLE : randomDouble(0, 1)",
        "HasPosInfs = (ii % 3 == 0) ? POS_INFINITY_DOUBLE : randomDouble(0, 1)",
        "HasNegInfs = (ii % 3 == 0) ? NEG_INFINITY_DOUBLE : randomDouble(0, 1)",
        "ReplacedNulls = replaceIfNull(HasNulls, -1)",
        "ReplacedNaNs = replaceIfNaN(HasNaNs, -1)",
        "ReplacedPosInfs = replaceIfNonFinite(HasPosInfs, -1)",
        "ReplacedNegInfs = replaceIfNonFinite(HasNegInfs, -1)",
    ]
)
```

### Filter out values

Use these [built-in functions](./built-in-functions.md) to [filter](./use-filters.md) out special values:

- [`isNull`](/core/javadoc/io/deephaven/function/Basic.html#isNull(double)) returns `true` for null values.
- [`isNaN`](/core/javadoc/io/deephaven/function/Numeric.html#isNaN(double)) returns `true` for NaN values.
- [`isInf`](/core/javadoc/io/deephaven/function/Numeric.html#isInf(double)) returns `true` for infinite values.
- [`isFinite`](/core/javadoc/io/deephaven/function/Numeric.html#isFinite(double)) returns `false` for null, NaN, and infinite values, so a single check removes all three.

The following example negates the first three functions with the [logical NOT operator](./operators.md#logical-operators) `!` to keep the rows without that value. It uses [`isFinite`](/core/javadoc/io/deephaven/function/Numeric.html#isFinite(double)) as is:

```python order=source,result_no_nulls,result_no_nans,result_no_nulls_nans,result_no_infs,result_finite
from deephaven import empty_table

source = empty_table(10).update(
    [
        "HasNulls = (ii % 3 == 0) ? NULL_DOUBLE : randomDouble(0, 1)",
        "HasNaNs = (ii % 4 == 2) ? NAN_DOUBLE : randomDouble(5, 10)",
        "HasInfs = (ii % 5 == 4) ? POS_INFINITY_DOUBLE : randomDouble(10, 15)",
    ]
)

result_no_nulls = source.where("!isNull(HasNulls)")
result_no_nans = source.where("!isNaN(HasNaNs)")
result_no_nulls_nans = source.where(["!isNull(HasNulls)", "!isNaN(HasNaNs)"])
result_no_infs = source.where("!isInf(HasInfs)")
result_finite = source.where(
    ["isFinite(HasNulls)", "isFinite(HasNaNs)", "isFinite(HasInfs)"]
)
```

### Python functions

To check for special values in a [Python function](./python-functions.md), test each input as follows:

- **Nulls, no `Optional` [type hint](./python-functions.md#parameter-type-hints):** compare the input against the null constants in [`deephaven.constants`](/core/pydoc/code/deephaven.constants.html), such as [`NULL_DOUBLE`](/core/pydoc/code/deephaven.constants.html).
- **Nulls, `Optional` [type hint](./python-functions.md#parameter-type-hints):** Deephaven passes a null input as `None`, so check `input_value is None`.
- **NaN, any [type hint](./python-functions.md#parameter-type-hints):** check with `math.isnan` instead of comparing against [`NAN_DOUBLE`](/core/pydoc/code/deephaven.constants.html), because NaN never equals itself.
- **Infinity:** compare against [`POS_INFINITY_DOUBLE`](/core/pydoc/code/deephaven.constants.html) and [`NEG_INFINITY_DOUBLE`](/core/pydoc/code/deephaven.constants.html), or use `math.isinf`.

The following function's parameter has no [type hint](./python-functions.md#parameter-type-hints), so it compares the input against [`NULL_DOUBLE`](/core/pydoc/code/deephaven.constants.html) and the infinity constants. Its `-> float` [return hint](./python-functions.md#type-hints) makes each result column a `double` column. The function returns a different value for each kind of input:

```python order=source,result
import math
from deephaven import empty_table
from deephaven.constants import (
    NULL_DOUBLE,
    POS_INFINITY_DOUBLE,
    NEG_INFINITY_DOUBLE,
)


def my_func(input_value) -> float:
    if input_value == NULL_DOUBLE:
        return -1.0
    elif math.isnan(input_value):
        return -2.0
    elif input_value == POS_INFINITY_DOUBLE:
        return -3.0
    elif input_value == NEG_INFINITY_DOUBLE:
        return -4.0
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

- [Deephaven's table types](../conceptual/table-types.md)
- [Create an empty table](./new-and-empty-table.md#empty_table)
- [Query string overview](./query-string-overview.md)
- [Query language built-in constants](./built-in-constants.md)
- [Built-in variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Filter table data](./use-filters.md)
- [Filters in query strings](./filters.md)
- [Formulas in query strings](./formulas.md)
- [Operators in query strings](./operators.md)
- [Ternary conditional operator](./ternary-if-how-to.md)
- [Python functions in query strings](./python-functions.md)
- [Auto-imported functions](../reference/query-language/query-library/auto-imported/index.md)
- [Basic functions](../reference/query-language/query-library/auto-imported/basic.md)
- [Constants](../reference/query-language/query-library/auto-imported/constants.md)
- [Math functions](../reference/query-language/query-library/auto-imported/math.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`meta_table`](../reference/table-operations/metadata/meta_table.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
- [Nulls](../reference/query-language/types/nulls.md)
- [NaNs](../reference/query-language/types/NaNs.md)
- [Pydoc](/core/pydoc/code/deephaven.constants.html)
