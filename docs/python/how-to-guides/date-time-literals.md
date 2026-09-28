---
title: Date-time literals in query strings
sidebar_label: Date-time
---

Date-time literals in query strings directly correspond to data types from the [java.time](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/package-summary.html) package. Deephaven leverages these Java date-time types to perform date-time arithmetic and comparisons within queries, allowing you to work with dates, times, durations, periods, and more using familiar and precise formats.

Proper handling of date-time values in queries is critical in Deephaven. For complete coverage of this topic, see [Work with time in Deephaven](../conceptual/time-in-deephaven.md).

This guide provides comprehensive coverage of date-time literals in query strings.

## What denotes a date-time literal?

In query strings, date-time literals are denoted by single quotes (`'`) around a string that represents a date-time data type such as a moment in time, duration, period, etc. Consider the following code:

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(1).update(
    [
        "InstantLiteral = '2025-05-01T00:00:00Z'",
        "DurationLiteral = 'PT5H2M1.2S'",
        "InstantPlusDuration = InstantLiteral + DurationLiteral",
    ]
)
source_meta = source.meta_table
```

Literal values are converted into date-time literals. If the conversion is successful, the value is treated as a date-time literal in the [formula](./formulas.md). If the value is improperly formatted, the conversion will fail and an error will be raised.

Deephaven highly recommends the use of double quotes (`"`) for query strings because single quotes (`'`) denote date-time and character literals. If you do use single quotes to enclose query strings, you must escape any enclosed single quotes, as shown by the example below:

```python order=source
from deephaven import empty_table

# The query string below is enclosed in single quotes
# As a result, the inner single quotes must be escaped
# This approach is less readable
source = empty_table(1).update(["InstantLiteral = '2025-05-01T00:00:00Z'"])
```

## Supported date-time literal formats

Deephaven supports many of the data types found in [java.time](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/package-summary.html) for date-time literals in query strings. Each supported type is covered in a subsection below:

### Instant

A [`java.time.Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) is a moment in time with nanosecond precision. It is represented as a string in [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601#) format, which includes the date and time, plus an optional time zone string.

In the UI, date-times are displayed in the local time zone. You can specify a desired time zone in the date-time literal itself. The following example creates two columns using instant literals with different time zone suffixes:

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(
    1
).update(
    [
        "InstantLiteral_UTC = '2023-10-01T09:30:00Z'",  # UTC (GMT+0)
        "InstantLiteral_NYC = '2025-01-23T15:21:48 ET'",  # Eastern Time (GMT-5 or GMT-6 depending on DST)
    ]
)
source_meta = source.meta_table
```

For more on the use of [`java.time.Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) in Deephaven, see [Natively supported date-time types](../conceptual/time-in-deephaven.md#natively-supported-date-time-types).

### LocalDate

A [`java.time.LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html) is a date without any time or time zone information. It follows the format:

`YYYY-MM-DD`

Where:

- `YYYY` is the four-digit year.
- `MM` is the two-digit month (01-12).
- `DD` is the two-digit day of the month (01-31).

The following code block creates a `LocalDate` column using a literal:

```python order=source
from deephaven import empty_table

source = empty_table(1).update(["LocalDateLiteral = '2025-04-01'"])
```

### LocalTime

A [`java.time.LocalTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalTime.html) is a time without any date or time zone information. It follows the format:

`hh:mm:ss.ddddddddd`

Where:

- `hh` is the two-digit hour (00-23).
- `mm` is the two-digit minute (00-59).
- `ss` is the two-digit second (00-59).
- `ddddddddd` is the optional fraction of a second in nanoseconds (0-999999999).

The following code block creates a `LocalTime` column using a literal:

```python order=source
from deephaven import empty_table

source = empty_table(1).update(["LocalTimeLiteral = '14:30:00.12345'"])
```

### Duration

A [`java.time.Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html) represents an amount of time expressed in hours, minutes, seconds, and nanoseconds. It follows the format:

`PTnHnMnS`

Where:

- `P` indicates the literal is a period or duration.
- `T` indicates the literal is a duration.
- `H` is the number of hours (optional).
- `M` is the number of minutes (optional).
- `S` is the number of seconds (optional).
- `n` is a non-negative number for each component.

The following code block creates a Duration column with a value of 5 hours, 2 minutes, and 1.2 seconds using a literal:

```python order=source
from deephaven import empty_table

source = empty_table(1).update(["DurationLiteral = 'PT5H2M1.2S'"])
```

### Period

A [`java.time.Period`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html), like a duration, represents a span of time. However, a period represents a whole number of years, months, and days. It follows the format:

`PnYnMnD`

Where:

- `P` indicates the period.
- `n` is a non-negative number for each component.
- `Y` is the number of years (optional).
- `M` is the number of months (optional).
- `D` is the number of days (optional).

The following code block creates a Period column with a value of 1 year, 4 months, and 5 days using a literal:

```python order=source
from deephaven import empty_table

source = empty_table(1).update(["PeriodLiteral = 'P1Y4M5D'"])
```

## Related documentation

- [Boolean and numeric literals](./boolean-numeric-literals.md)
- [String and char literals](./string-char-literals.md)
- [Time in Deephaven](../conceptual/time-in-deephaven.md)
