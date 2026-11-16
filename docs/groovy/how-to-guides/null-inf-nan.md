---
title: Null, NaN, and infinity values
---

Null, not-a-number (NaN), and infinity values can unexpectedly appear in your datasets and disrupt calculations if you don't handle them. In a [real-time table](../conceptual/table-types.md), any update can bring in a new one, so it pays to handle them in the query itself.

This guide shows you how to keep your data analysis running smoothly. It lists the constants Deephaven provides for these special values, then shows how [built-in functions](./built-in-functions.md) treat them and how to replace them, [filter](./use-filters.md) them out, and check for them in your own [Groovy closures](./groovy-closures.md).

**Nulls** represent missing data. For a [primitive](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) numeric type or `char`, Deephaven reserves one value of the type as its null. For example, it reserves `-32768` as the null value for the primitive `short` data type, so the minimum `short` value is `-32767` instead of the traditional `-32768`. Booleans and object types use Java `null` instead.

**NaNs** represent undefined mathematical results. For instance, the logarithm of a negative number is undefined, so [`log`](/core/javadoc/io/deephaven/function/Numeric.html#log(double)) returns NaN for a negative input.

**Infinities** represent mathematically infinite values. For example, in floating-point arithmetic, dividing a positive number by `0.0` produces positive infinity.

## Available constants

Deephaven provides a null constant for each primitive numeric type and `char`, plus [`NULL_BOOLEAN`](./built-in-constants.md#null-constants), which equals Java `null`, for `Boolean` columns. Only `float` and `double` have NaN and infinity constants.

The constants are [built into the query language](./built-in-constants.md), so you can use them in Groovy code and in [query strings](./query-string-overview.md) without an import. In each subsection below, the first example prints the constants from Groovy, and the second uses them in a [query string](./query-string-overview.md) and shows the column types with [`meta`](../reference/table-operations/metadata/meta.md).

### Nulls

The following null constants are available in Deephaven:

```groovy order=:log
nulls = [
    "boolean": NULL_BOOLEAN,
    "char": (int) NULL_CHAR,
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

The `char` null is cast to `int` so that it prints as a number instead of an unprintable character.

For `float` and `double`, the null value printed above is the most negative finite value of the type. The smallest non-null finite value is one step above it. For `double`, that value is [`MIN_FINITE_DOUBLE`](./built-in-constants.md#minimum-and-maximum-constants).

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

## Handle null, NaN, and infinity values

Each example in this section builds columns that hold special values in selected rows. To choose those rows, the examples use the [ternary conditional operator](./ternary-if-how-to.md) with [`ii`](../reference/query-language/variables/special-variables.md), the [built-in variable](./built-in-variables.md) that holds the row position.

### Built-in functions

The [functions built into the query language](./built-in-functions.md) accept null, NaN, and infinity values without extra checks in your query, but they treat each kind differently:

- **Null:** most functions skip null inputs or return null.
- **NaN and infinity:** functions follow IEEE floating-point rules, so a NaN or infinite input usually gives a NaN or infinite result.

For example, [`avg`](/core/javadoc/io/deephaven/function/Numeric.html#avg(double...)) skips null values but returns NaN for an array that contains a NaN.

The following example passes each kind of value to the built-in [`sin`](/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) function. Every fourth row holds the special value:

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

Where the input is null, `Y1` is null. Where the input is NaN or infinite, `Y2`, `Y3`, and `Y4` are NaN.

Arithmetic [operators](./operators.md#arithmetic-operators) in [query strings](./query-string-overview.md) treat nulls the same way: if either operand is null, the result is null. To test for a null, use `X == NULL_DOUBLE`. To test for a NaN, use [`isNaN(X)`](/core/javadoc/io/deephaven/function/Numeric.html#isNaN(double)), because `X == NAN_DOUBLE` is never `true`. [Comparison operators](./operators.md#comparison-operators) covers the other null and NaN comparison rules.

### Replace values

Replacing null, NaN, and infinity values is a common practice when a dataset should have a default value. The following code block uses [built-in functions](./built-in-functions.md) to replace each kind of value with `-1`:

- [`replaceIfNull`](/core/javadoc/io/deephaven/function/Basic.html#replaceIfNull(double,double)) replaces null values.
- [`replaceIfNaN`](/core/javadoc/io/deephaven/function/Numeric.html#replaceIfNaN(double,double)) replaces NaN values.
- [`replaceIfNullNaN`](/core/javadoc/io/deephaven/function/Numeric.html#replaceIfNullNaN(double,double)) replaces null and NaN values, but not infinite values.
- [`replaceIfNonFinite`](/core/javadoc/io/deephaven/function/Numeric.html#replaceIfNonFinite(double,double)) replaces infinite values. It also replaces null and NaN values.

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

### Filter out values

Use these [built-in functions](./built-in-functions.md) to [filter](./use-filters.md) out special values:

- [`isNull`](/core/javadoc/io/deephaven/function/Basic.html#isNull(double)) returns `true` for null values.
- [`isNaN`](/core/javadoc/io/deephaven/function/Numeric.html#isNaN(double)) returns `true` for NaN values.
- [`isInf`](/core/javadoc/io/deephaven/function/Numeric.html#isInf(double)) returns `true` for infinite values.
- [`isFinite`](/core/javadoc/io/deephaven/function/Numeric.html#isFinite(double)) returns `false` for null, NaN, and infinite values, so a single check removes all three.

The following example negates the first three functions with the [logical NOT operator](./operators.md#logical-operators) `!` to keep the rows without that value. It uses [`isFinite`](/core/javadoc/io/deephaven/function/Numeric.html#isFinite(double)) as is:

```groovy order=source,resultNoNulls,resultNoNans,resultNoNullsNans,resultNoInfs,resultFinite
source = emptyTable(10).update(
    "HasNulls = (ii % 3 == 0) ? NULL_DOUBLE : randomDouble(0, 1)",
    "HasNaNs = (ii % 4 == 2) ? NAN_DOUBLE : randomDouble(5, 10)",
    "HasInfs = (ii % 5 == 4) ? POS_INFINITY_DOUBLE : randomDouble(10, 15)",
)

resultNoNulls = source.where("!isNull(HasNulls)")
resultNoNans = source.where("!isNaN(HasNaNs)")
resultNoNullsNans = source.where("!isNull(HasNulls)", "!isNaN(HasNaNs)")
resultNoInfs = source.where("!isInf(HasInfs)")
resultFinite = source.where("isFinite(HasNulls)", "isFinite(HasNaNs)", "isFinite(HasInfs)")
```

### Groovy closures

To check for null or infinite inputs in a [Groovy closure](./groovy-closures.md), compare each input against the [built-in constants](./built-in-constants.md).

Check for NaN with `Double.isNaN` instead of comparing against [`NAN_DOUBLE`](./built-in-constants.md#nan-constants). In Java, NaN never equals itself. Groovy's `==` doesn't always follow that rule: in a closure like the one below, `inputValue == NAN_DOUBLE` returns `true` for a NaN input, but the same comparison between two `double` variables returns `false`. `Double.isNaN` returns the right answer in both cases.

The following closure returns a different value for each kind of input. Without a cast, a [closure call produces an `Object` column](./groovy-closures.md#call-a-closure-in-a-query-string), so each [query string](./query-string-overview.md) [casts](./casting.md#groovy-closures) the closure's result to `double`:

```groovy order=source,result
myFunc = { double inputValue ->
    if (inputValue == NULL_DOUBLE) {
        return -1.0d
    } else if (Double.isNaN(inputValue)) {
        return -2.0d
    } else if (inputValue == POS_INFINITY_DOUBLE) {
        return -3.0d
    } else if (inputValue == NEG_INFINITY_DOUBLE) {
        return -4.0d
    } else {
        return inputValue * 2
    }
}

source = emptyTable(10).update(
    "HasNulls = (ii % 3 == 0) ? NULL_DOUBLE : randomDouble(0, 1)",
    "HasNaNs = (ii % 4 == 2) ? NAN_DOUBLE : randomDouble(5, 10)",
    "HasPosInfs = (ii % 5 == 4) ? POS_INFINITY_DOUBLE : randomDouble(10, 15)",
    "HasNegInfs = (ii % 6 == 5) ? NEG_INFINITY_DOUBLE : randomDouble(15, 20)",
)

result = source.update(
    "ResultNulls = (double) myFunc(HasNulls)",
    "ResultNaNs = (double) myFunc(HasNaNs)",
    "ResultPosInfs = (double) myFunc(HasPosInfs)",
    "ResultNegInfs = (double) myFunc(HasNegInfs)",
)
```

## Related documentation

- [Deephaven's table types](../conceptual/table-types.md)
- [Create an empty table](./new-and-empty-table.md#emptytable)
- [Query string overview](./query-string-overview.md)
- [Query language built-in constants](./built-in-constants.md)
- [Built-in variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Filter table data](./use-filters.md)
- [Filters in query strings](./filters.md)
- [Formulas in query strings](./formulas.md)
- [Operators in query strings](./operators.md)
- [Ternary conditional operator](./ternary-if-how-to.md)
- [Groovy closures in queries](./groovy-closures.md)
- [Casting](./casting.md)
- [Auto-imported functions](../reference/query-language/query-library/auto-imported/index.md)
- [Basic functions](../reference/query-language/query-library/auto-imported/basic.md)
- [Constants](../reference/query-language/query-library/auto-imported/constants.md)
- [Math functions](../reference/query-language/query-library/auto-imported/math.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`meta`](../reference/table-operations/metadata/meta.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
- [Nulls](../reference/query-language/types/nulls.md)
- [NaNs](../reference/query-language/types/NaNs.md)
- [Javadoc](/core/javadoc/io/deephaven/util/QueryConstants.html)
