---
title: Query language built-in constants
---

The Deephaven Query Language (DQL) has a large number of built-in constants that you can use in [query strings](./query-string-overview.md) with no imports or setup. This guide covers these groups of built-in constants:

- [Null values](../reference/query-language/types/nulls.md).
- Minimum and maximum allowed values.
- [Infinities](./null-inf-nan.md).
- [NaNs](../reference/query-language/types/NaNs.md), or Not a Number values.
- Lengths of time and time-unit conversion factors.
- The math constants `E` and `PI`.

The constants in this guide use upper snake case, such as `NULL_INT`, following the Java naming convention for constants. For every constant with its type, see [Auto-imported constants](../reference/query-language/query-library/auto-imported/constants.md).

[Query strings](./query-string-overview.md) also have built-in named colors, such as `BLUE`, for [table formatting](./format-columns.md). See [Auto-imported GUI functions](../reference/query-language/query-library/auto-imported/gui.md) for the list.

Two more built-in fields set how [query strings](./query-string-overview.md) divide `BigDecimal` and `BigInteger` values. The [Other built-in fields](#other-built-in-fields) section at the end of this guide describes them.

> [!NOTE]
> Many built-in constants also have Python versions for use in Python code. The [`deephaven.constants` Pydoc](/core/pydoc/code/deephaven.constants.html) covers the Python null, limit, NaN, and infinity constants.
>
> In [query strings](./query-string-overview.md), use the built-in constants without importing them from [`deephaven.constants`](/core/pydoc/code/deephaven.constants.html). A [Python variable](./python-variables.md) with the same name as a built-in constant takes precedence over the built-in, and the result is no longer [null](../reference/query-language/types/nulls.md):
>
> - After `from deephaven.constants import NULL_FLOAT`, `"F = NULL_FLOAT"` makes a `double` column with the value `-Float.MAX_VALUE`.
> - After `from deephaven.constants import NULL_CHAR`, `"C = NULL_CHAR"` makes an `int` column with the value `65535`.
>
> Neither value is [null](../reference/query-language/types/nulls.md) in its column's type, so [`isNull`](../reference/query-language/query-library/auto-imported/basic.md) returns `false`. To use the Python constants in Python code without this conflict, import the module with `from deephaven import constants`, and write names such as `constants.NULL_FLOAT`.

## Null constants

Each [Java primitive data type](./data-types.md) except `boolean` has a constant for its [null value](../reference/query-language/types/nulls.md). `NULL_BOOLEAN` is the [null value](../reference/query-language/types/nulls.md) for `Boolean` columns, and it equals `null`. The query below shows the primitive null constants and `NULL_BOOLEAN`:

```python order=source
from deephaven import empty_table

source = empty_table(1).update(
    [
        "NullBool = NULL_BOOLEAN",
        "NullByte = NULL_BYTE",
        "NullChar = NULL_CHAR",
        "NullDouble = NULL_DOUBLE",
        "NullFloat = NULL_FLOAT",
        "NullInt = NULL_INT",
        "NullLong = NULL_LONG",
        "NullShort = NULL_SHORT",
    ]
)
```

Each cell holds its column type's [null value](../reference/query-language/types/nulls.md), which the table displays as an empty cell.

Boxed forms such as `NULL_INT_BOXED` also exist for the numeric and `char` types. They hold the same [null value](../reference/query-language/types/nulls.md) as a Java wrapper object, such as `Integer`, instead of a primitive.

To check for nulls or replace them in a query, see [Null, NaN, and infinity values](./null-inf-nan.md).

## Minimum and maximum constants

Every [Java primitive type](./data-types.md) except `boolean` has constants for its minimum and maximum non-[null](../reference/query-language/types/nulls.md) values. `MAX_BYTE`, `MAX_SHORT`, `MAX_INT`, `MAX_LONG`, and `MIN_CHAR` equal Java's limits. The rest differ from Java's limits for one of two reasons:

- Deephaven reserves one value of each numeric and `char` type to represent [null](../reference/query-language/types/nulls.md):
  - `MIN_BYTE`, `MIN_SHORT`, `MIN_INT`, and `MIN_LONG` are one greater than Java's minimum, because Java's minimum is the [null value](../reference/query-language/types/nulls.md). For example, `MIN_INT` is `Integer.MIN_VALUE + 1`.
  - `MAX_CHAR` is `Character.MAX_VALUE - 1`, because `Character.MAX_VALUE` is `NULL_CHAR`.
- Deephaven defines the floating-point minimum and maximum as [infinities](./null-inf-nan.md). `MAX_DOUBLE` and `MAX_FLOAT` are positive infinity, and `MIN_DOUBLE` and `MIN_FLOAT` are negative infinity.

Because the floating-point minimum and maximum are [infinities](./null-inf-nan.md), Deephaven also provides finite limits:

- `MAX_FINITE_DOUBLE` and `MAX_FINITE_FLOAT` are the largest finite values, `Double.MAX_VALUE` and `Float.MAX_VALUE`.
- `MIN_FINITE_DOUBLE` and `MIN_FINITE_FLOAT` are the most negative finite values that aren't [null](../reference/query-language/types/nulls.md). Deephaven uses `-Double.MAX_VALUE` and `-Float.MAX_VALUE` as `NULL_DOUBLE` and `NULL_FLOAT`, so each `MIN_FINITE_*` constant is the next value above its type's [null value](../reference/query-language/types/nulls.md).
- `MIN_POS_DOUBLE` and `MIN_POS_FLOAT` are the smallest positive values, `Double.MIN_VALUE` and `Float.MIN_VALUE`.

The query below uses these constants in [query strings](./query-string-overview.md):

```python order=source
from deephaven import empty_table

source = empty_table(1).update(
    [
        "MaxByte = MAX_BYTE",
        "MaxChar = MAX_CHAR",
        "MaxDouble = MAX_DOUBLE",
        "MaxFiniteDouble = MAX_FINITE_DOUBLE",
        "MaxFiniteFloat = MAX_FINITE_FLOAT",
        "MaxFloat = MAX_FLOAT",
        "MaxInt = MAX_INT",
        "MaxLong = MAX_LONG",
        "MaxShort = MAX_SHORT",
        "MinByte = MIN_BYTE",
        "MinChar = MIN_CHAR",
        "MinDouble = MIN_DOUBLE",
        "MinFiniteDouble = MIN_FINITE_DOUBLE",
        "MinFiniteFloat = MIN_FINITE_FLOAT",
        "MinFloat = MIN_FLOAT",
        "MinInt = MIN_INT",
        "MinLong = MIN_LONG",
        "MinPosDouble = MIN_POS_DOUBLE",
        "MinPosFloat = MIN_POS_FLOAT",
        "MinShort = MIN_SHORT",
    ]
)
```

The table's default number format rounds to four decimal places, so `MinPosDouble` and `MinPosFloat` display as `0.0000` even though they are greater than zero. `MinChar` and `MaxChar` are unprintable characters, so their cells can look empty even though neither is [null](../reference/query-language/types/nulls.md).

## Infinity constants

Both Java floating-point types, `float` and `double`, have positive and negative [infinity](./null-inf-nan.md) constants. Each one equals a floating-point limit:

- `POS_INFINITY_FLOAT` equals `MAX_FLOAT`.
- `NEG_INFINITY_FLOAT` equals `MIN_FLOAT`.
- `POS_INFINITY_DOUBLE` equals `MAX_DOUBLE`.
- `NEG_INFINITY_DOUBLE` equals `MIN_DOUBLE`.

The [Minimum and maximum constants](#minimum-and-maximum-constants) section above explains why. The query below uses all four [infinity](./null-inf-nan.md) constants in [query strings](./query-string-overview.md):

```python order=source
from deephaven import empty_table

source = empty_table(1).update(
    [
        "PositiveInfinityFloat = POS_INFINITY_FLOAT",
        "NegativeInfinityFloat = NEG_INFINITY_FLOAT",
        "PositiveInfinityDouble = POS_INFINITY_DOUBLE",
        "NegativeInfinityDouble = NEG_INFINITY_DOUBLE",
    ]
)
```

## NaN constants

A [NaN](../reference/query-language/types/NaNs.md) (Not a Number) value is a special floating-point value that represents an undefined or unrepresentable value. [NaNs](../reference/query-language/types/NaNs.md) only exist for the floating-point types, `float` and `double`. The query below uses the [NaN](../reference/query-language/types/NaNs.md) constants in [query strings](./query-string-overview.md):

```python order=source
from deephaven import empty_table

source = empty_table(1).update(
    [
        "NanFloat = NAN_FLOAT",
        "NanDouble = NAN_DOUBLE",
    ]
)
```

## Date-time constants

Most built-in date-time constants, such as `SECOND` and `DAY`, are lengths of time stored as `long` nanosecond counts, not `Duration` objects:

- `MICRO`: One microsecond in nanoseconds.
- `MILLI`: One millisecond in nanoseconds.
- `SECOND`: One second in nanoseconds.
- `MINUTE`: One minute in nanoseconds.
- `HOUR`: One hour in nanoseconds.
- `DAY`: One day in nanoseconds.
- `WEEK`: One week in nanoseconds.
- `YEAR_365`: One year in nanoseconds (365 days).
- `YEAR_AVG`: One year in nanoseconds (average, 365.2425 days).

These constants are fixed lengths of time that don't account for calendar adjustments. `DAY` is always 24 hours, even across a daylight saving time change. `YEAR_365` is always 365 days, even in a leap year.

The conversion factors are `double` values that turn a nanosecond count into another unit. Multiply a count by a factor to convert it. For example, multiplying by `DAYS_PER_NANO` converts nanoseconds to days. The conversion factors are:

- `SECONDS_PER_NANO`: The number of seconds per nanosecond.
- `MINUTES_PER_NANO`: The number of minutes per nanosecond.
- `HOURS_PER_NANO`: The number of hours per nanosecond.
- `DAYS_PER_NANO`: The number of days per nanosecond.
- `YEARS_PER_NANO_365`: The number of years per nanosecond (365 days).
- `YEARS_PER_NANO_AVG`: The number of years per nanosecond (average, 365.2425 days).

One date-time constant is neither a length of time nor a conversion factor: `ZERO_LENGTH_INSTANT_ARRAY` is a zero-length [`Instant`](../reference/query-language/types/date-time.md#instant) array.

The query below adds multiples of the duration constants to a timestamp. The last two columns use [`diffNanos`](../reference/query-language/query-library/auto-imported/time.md) and conversion factors to express elapsed time in days and in average years:

```python order=source
from deephaven import empty_table

source = empty_table(10).update(
    [
        "Timestamp = '2023-10-01T00:00:00Z'",
        "OneSecondAtATime = Timestamp + ii * SECOND",
        "OneMinuteAtATime = Timestamp + ii * MINUTE",
        "OneHourAtATime = Timestamp + ii * HOUR",
        "OneDayAtATime = Timestamp + ii * DAY",
        "OneWeekAtATime = Timestamp + ii * WEEK",
        "OneYearAtATime365 = Timestamp + ii * YEAR_365",
        "OneYearAtATimeAvg = Timestamp + ii * YEAR_AVG",
        "ElapsedDays = diffNanos(Timestamp, OneHourAtATime) * DAYS_PER_NANO",
        "ElapsedAvgYears = diffNanos(Timestamp, OneYearAtATime365) * YEARS_PER_NANO_AVG",
    ]
)
```

`ElapsedDays` grows by 1/24 of a day per row. `ElapsedAvgYears` falls slightly behind [`ii`](../reference/query-language/variables/special-variables.md), because a 365-day year is shorter than an average year of 365.2425 days.

## Math constants

Deephaven has only two built-in math constants:

- `E`: The base of the natural logarithm, approximately 2.718281828459045.
- `PI`: The ratio of a circle's circumference to its diameter, approximately 3.141592653589793.

The query below uses `PI` to compute the circumference and area of circles, and uses [`log`](../reference/query-language/query-library/auto-imported/math.md) to confirm that the natural logarithm of `E` is 1:

```python order=source
from deephaven import empty_table

source = empty_table(5).update(
    [
        "Radius = ii + 1",
        "Circumference = 2 * PI * Radius",
        "Area = PI * pow(Radius, 2)",
        "LogE = log(E)",
    ]
)
```

## Other built-in fields

Two more built-in fields control how [query strings](./query-string-overview.md) divide `BigDecimal` and `BigInteger` values. They are settings, not data values:

- `DEFAULT_SCALE`: The minimum scale of a division result. Scale is the number of digits after the decimal point. The default is 8. To change it, set the `QueryLanguageFunctionUtils.defaultScale` [configuration property](./configuration/configuration-properties.md#how-to-set-properties).
- `ROUNDING_MODE`: The rounding mode for division, `HALF_UP`.

## Related documentation

- [Work with time](../conceptual/time-in-deephaven.md)
- [Apply color formatting to columns](./format-columns.md)
- [Built-in variables](./built-in-variables.md)
- [Built-in functions](./built-in-functions.md)
- [Configuration properties](./configuration/configuration-properties.md)
- [Data types](./data-types.md)
- [Null, NaN, and infinity values](./null-inf-nan.md)
- [Query string overview](./query-string-overview.md)
- [Auto-imported functions](../reference/query-language/query-library/auto-imported/index.md)
- [Auto-imported basic functions](../reference/query-language/query-library/auto-imported/basic.md)
- [Auto-imported GUI functions](../reference/query-language/query-library/auto-imported/gui.md)
- [Auto-imported constants](../reference/query-language/query-library/auto-imported/constants.md)
- [Auto-imported math functions](../reference/query-language/query-library/auto-imported/math.md)
- [Auto-imported time functions](../reference/query-language/query-library/auto-imported/time.md)
- [NaNs](../reference/query-language/types/NaNs.md)
- [Nulls](../reference/query-language/types/nulls.md)
- [Date-time](../reference/query-language/types/date-time.md)
- [Special variables and constants](../reference/query-language/variables/special-variables.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`deephaven.constants` Pydoc](/core/pydoc/code/deephaven.constants.html)
- [`DateTimeUtils` Javadoc](/core/javadoc/io/deephaven/time/DateTimeUtils.html)
- [`Numeric` Javadoc](/core/javadoc/io/deephaven/function/Numeric.html)
