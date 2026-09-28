---
title: Query language built-in constants
sidebar_label: Built-in constants
---

Deephaven's Query Language (DQL) has a large number of built-in constants. These can be used in queries with no additional imports or setup. The built-in constants cover null values, maximum and minimum allowed values, NaNs, infinities, date-time values, and more. All constants use the `CAPITAL_CASE` naming convention to distinguish them from user-defined variables.

## Null constants

There are constant values representing the null value for each Java primitive data type. The query below shows null constant values for all primitive types:

```groovy order=source
source = emptyTable(1).update(
    "NullBool = NULL_BOOLEAN",
    "NullByte = NULL_BYTE",
    "NullChar = NULL_CHAR",
    "NullDouble = NULL_DOUBLE",
    "NullFloat = NULL_FLOAT",
    "NullInt = NULL_INT",
    "NullLong = NULL_LONG",
    "NullShort = NULL_SHORT",
)
```

## Minimum and maximum constants

There are constant values representing the minimum and maximum values for each Java primitive data type. The query below shows the use of these constants in query strings:

```groovy order=source
source = emptyTable(1).update(
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
)
```

## NaN constants

A NaN (Not a Number) value is a special floating-point value that represents an undefined or unrepresentable value. NaNs only exist for floating-point types (`float` and `double`). The query below shows the use of NaN constants in query strings:

```groovy order=source
source = emptyTable(1).update(
    "NanFloat = NAN_FLOAT",
    "NanDouble = NAN_DOUBLE",
)
```

## Infinities

There are constants for both positive and negative infinity for both Java primitive floating-point types (`float` and `double`). The query below shows the use of all four infinity constants in query strings:

```groovy order=source
source = emptyTable(1).update(
    "PositiveInfinityFloat = POS_INFINITY_FLOAT",
    "NegativeInfinityFloat = NEG_INFINITY_FLOAT",
    "PositiveInfinityDouble = POS_INFINITY_DOUBLE",
    "NegativeInfinityDouble = NEG_INFINITY_DOUBLE",
)
```

## Date-time constants

There are several built-in date-time constants available. These constants represent numeric values in nanoseconds that can be used for common date-time operations:

- `DAY`: One day in nanoseconds.
- `DAYS_PER_NANO`: The number of days per nanosecond.
- `HOUR`: One hour in nanoseconds.
- `HOURS_PER_NANO`: The number of hours per nanosecond.
- `MICRO`: One microsecond in nanoseconds.
- `MILLI`: One millisecond in nanoseconds.
- `MINUTE`: One minute in nanoseconds.
- `MINUTES_PER_NANO`: The number of minutes per nanosecond.
- `SECOND`: One second in nanoseconds.
- `SECONDS_PER_NANO`: The number of seconds per nanosecond.
- `WEEK`: One week in nanoseconds.
- `YEAR_365`: One year in nanoseconds (365 days).
- `YEAR_AVG`: One year in nanoseconds (average, 365.2425 days).
- `YEARS_PER_NANO_365`: The number of years per nanosecond (365 days).
- `YEARS_PER_NANO_AVG`: The number of years per nanosecond (average, 365.2425 days).
- `ZERO_LENGTH_INSTANT_ARRAY`: A zero-length instant array, used for initializing date-time arrays.

The query below uses some of these constants to demonstrate simple use cases:

```groovy order=source
source = emptyTable(10).update(
    "Timestamp = '2023-10-01T00:00:00Z'",
    "OneSecondAtATime = Timestamp + ii * SECOND",
    "OneMinuteAtATime = Timestamp + ii * MINUTE",
    "OneHourAtATime = Timestamp + ii * HOUR",
    "OneDayAtATime = Timestamp + ii * DAY",
    "OneWeekAtATime = Timestamp + ii * WEEK",
    "OneYearAtATime365 = Timestamp + ii * YEAR_365",
    "OneYearAtATimeAvg = Timestamp + ii * YEAR_AVG",
)
```

## Math constants

Deephaven has only two built-in math constants:

- `E`: The base of the natural logarithm, approximately 2.718281828459045.
- `PI`: The ratio of a circle's circumference to its diameter, approximately 3.141592653589793.

## Related documentation

- [Built-in variables](./built-in-variables.md)
- [Built-in functions](./built-in-functions.md)
- [Handle nulls, infinities, and NaNs](./null-inf-nan.md)
- [Work with date-times](../conceptual/time-in-deephaven.md)
- [Auto-imported functions](../reference/query-language/query-library/auto-imported/index.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`update`](../reference/table-operations/select/update.md)
