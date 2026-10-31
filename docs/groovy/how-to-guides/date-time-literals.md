---
title: Date-time literals in query strings
---

Date-time literals in [query strings](./query-string-overview.md) directly correspond to data types from the [`java.time`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/package-summary.html) package. Deephaven uses these Java types for date-time arithmetic and comparisons in queries. You can write dates, times, durations, periods, and time zones directly in a [query string](./query-string-overview.md), using familiar and precise formats.

Proper handling of date-time values in queries is critical in Deephaven. For complete coverage of this topic, see [Work with time](../conceptual/time-in-deephaven.md).

This guide shows how Deephaven recognizes date-time literals and the formats it accepts for each type.

## What denotes a date-time literal?

In [query strings](./query-string-overview.md), single quotes (`'`) around a string mark a date-time literal. The string represents a date-time value, such as a moment in time, a duration, or a period. Consider the following code:

```groovy order=source,sourceMeta
source = emptyTable(1).update(
    "InstantLiteral = '2025-05-01T00:00:00Z'",
    "DurationLiteral = 'PT5H2M1.2S'",
    "InstantPlusDuration = InstantLiteral + DurationLiteral",
)
sourceMeta = source.meta()
```

Deephaven parses the quoted string into the matching [`java.time`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/package-summary.html) object, which the [formula](./formulas.md) then uses like any other value. The `sourceMeta` table shows each column's type:

- `InstantLiteral` is an [`Instant`](../reference/query-language/types/date-time.md#instant).
- `DurationLiteral` is a `Duration`.
- `InstantPlusDuration`, the sum of the two, is another [`Instant`](../reference/query-language/types/date-time.md#instant).

If the string doesn't match a supported format, the query fails with an error.

Deephaven recommends double quotes (`"`) for [query strings](./query-string-overview.md), because single quotes (`'`) denote date-time literals and [character literals](./string-char-literals.md), such as `'a'`. If you do use single quotes to enclose query strings, you must escape any enclosed single quotes, as shown by the example below:

```groovy order=source
// The query string below is enclosed in single quotes
// As a result, the inner single quotes must be escaped
// This approach is less readable
source = emptyTable(1).update('InstantLiteral = \'2025-05-01T00:00:00Z\'')
```

## Supported date-time literal formats

Deephaven supports many of the data types found in [`java.time`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/package-summary.html) for date-time literals in [query strings](./query-string-overview.md). In the formats below, square brackets mark optional parts, and each type's subsection explains its placeholder letters:

| Java type                 | Format                                                                                                                               | Example literal                                                      |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------- |
| [`Instant`](#instant)     | `YYYY-MM-DDThh:mm[:ss[.nnnnnnnnn]]` and a time zone; `YYYY-MM-DD` and a time zone name or alias; or a whole-number Unix epoch offset | `'2023-10-01T09:30:00Z'`, `'2025-01-23T15:21:48 ET'`, `'1700000000'` |
| [`LocalDate`](#localdate) | `YYYY-MM-DD`                                                                                                                         | `'2025-04-01'`                                                       |
| [`LocalTime`](#localtime) | `hh:mm[:ss[.nnnnnnnnn]]`                                                                                                             | `'14:30:00.12345'`                                                   |
| [`Duration`](#duration)   | `[-]PnDTnHnMn.nS` or `[-]PThh:mm[:ss[.nnnnnnnnn]]`                                                                                   | `'PT5H2M1.2S'`, `'PT12:00'`                                          |
| [`Period`](#period)       | `[-]PnYnMnWnD`                                                                                                                       | `'P1Y4M5D'`                                                          |
| [`ZoneId`](#zoneid)       | Time zone name, alias, or offset                                                                                                     | `'America/New_York'`, `'ET'`, `'-05:00'`                             |

A single character in single quotes, such as `'a'`, is a [character literal](./string-char-literals.md). Deephaven tries to parse any longer string as each of the following types, in this order. The first type that parses the string determines the literal's type:

1. [`Instant`](#instant)
2. [`Period`](#period)
3. [`Duration`](#duration)
4. [`LocalDate`](#localdate)
5. [`LocalTime`](#localtime)
6. [`ZoneId`](#zoneid)

For example, `'P2D'` is a valid `Period` and a valid `Duration`, so it becomes a `Period`.

[`ZonedDateTime`](../reference/query-language/types/date-time.md#zoneddatetime) and [`LocalDateTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDateTime.html) have no literal form. A date-time string with a time zone becomes an [`Instant`](../reference/query-language/types/date-time.md#instant). A date and time with no time zone, such as `'2025-01-23T15:21:48'`, isn't a valid literal, and the query fails.

The following subsections cover each type.

### LocalDate

A [`java.time.LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html) is a date without any time or time zone information. It follows the format:

`YYYY-MM-DD`

Where:

- `YYYY` is the four-digit year.
- `MM` is the month (1-12), written with one or two digits.
- `DD` is the day of the month (1-31), written with one or two digits.

The following code block creates a `LocalDate` column using a literal:

```groovy order=source
source = emptyTable(1).update("LocalDateLiteral = '2025-04-01'")
```

### LocalTime

A [`java.time.LocalTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalTime.html) is a time without any date or time zone information. It follows the format:

`hh:mm[:ss[.nnnnnnnnn]]`

Where:

- `hh` is the hour (0-23), written with one or two digits.
- `mm` is the minute (0-59), written with one or two digits.
- `ss` is the optional second (0-59), written with one or two digits.
- `nnnnnnnnn` is the optional fraction of a second, with up to nine digits (nanosecond precision). It can appear only after `ss`.

The following code block creates a `LocalTime` column using a literal:

```groovy order=source
source = emptyTable(1).update("LocalTimeLiteral = '14:30:00.12345'")
```

### Instant

A [`java.time.Instant`](../reference/query-language/types/date-time.md#instant) is a moment in time with nanosecond precision. An [`Instant`](../reference/query-language/types/date-time.md#instant) literal takes one of three forms:

- A date and time followed by a time zone, such as `'2023-10-01T09:30:00Z'` or `'2025-01-23T15:21:48 ET'`. The date and time use the [`LocalDate`](#localdate) and [`LocalTime`](#localtime) formats described above, joined by `T`.
- A date followed by a space and a time zone name or alias, such as `'2025-01-23 ET'`. This form is midnight on that date in that time zone. The month and day must each have two digits, and the time zone can't be `Z` or an offset.
- A whole number, such as `'1700000000'`, which is an offset from the [Unix epoch](https://en.wikipedia.org/wiki/Unix_time). Deephaven infers whether the number is in seconds, milliseconds, microseconds, or nanoseconds from its size.

For the first form, write the time zone in one of two ways:

- `Z` or an [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) offset, such as `-05:00`, directly after the time. In this form, the month, day, hour, minute, and second must each have two digits.
- A time zone name, a Deephaven [time zone alias](../conceptual/time-in-deephaven.md#time-zones-and-aliases), or an offset, such as `America/New_York`, `ET`, or `-05:00`, after a space.

The UI displays date-times in the time zone selected in the [**Settings** menu](./set-date-time-format.md), regardless of the time zone written in the literal.

The following example creates two columns from [`Instant`](../reference/query-language/types/date-time.md#instant) literals with different time zones. The `sourceMeta` table shows that both columns are [`Instant`](../reference/query-language/types/date-time.md#instant) columns:

```groovy order=source,sourceMeta
source = emptyTable(1).update(
    "InstantLiteral_UTC = '2023-10-01T09:30:00Z'",  // UTC (GMT+0)
    "InstantLiteral_NYC = '2025-01-23T15:21:48 ET'",  // Eastern Time (GMT-5, or GMT-4 during DST)
)
sourceMeta = source.meta()
```

For more on how Deephaven uses [`Instant`](../reference/query-language/types/date-time.md#instant) values, see [Natively supported date-time types](../conceptual/time-in-deephaven.md#natively-supported-date-time-types).

### Duration

A [`java.time.Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html) represents an amount of time expressed in days, hours, minutes, seconds, and nanoseconds. It follows the format:

`[-]PnDTnHnMn.nS`

Where:

- `-` negates the whole duration (optional).
- `P` indicates the literal is a period or duration.
- `nD` is the number of days, each exactly 24 hours (optional).
- `T` separates the days from the time components.
- `nH` is the number of hours (optional).
- `nM` is the number of minutes (optional).
- `n.nS` is the number of seconds, which can include a decimal fraction (optional).
- `n` is a number, which can be negative.

A literal with only days and no `T`, such as `'P2D'`, becomes a `Period` instead, because `Period` comes before `Duration` in the [parse order](#supported-date-time-literal-formats). To write two days as a `Duration`, use `'PT48H'`.

Deephaven also accepts a clock-style duration, `[-]PThh:mm[:ss[.nnnnnnnnn]]`, such as `'PT12:00'` for 12 hours or `'PT1:30:15.5'` for 1 hour, 30 minutes, and 15.5 seconds.

The following code block creates a `Duration` column with a value of 5 hours, 2 minutes, and 1.2 seconds using a literal:

```groovy order=source
source = emptyTable(1).update("DurationLiteral = 'PT5H2M1.2S'")
```

### Period

A [`java.time.Period`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html), like a duration, represents a span of time. However, a period represents a whole number of years, months, and days. It follows the format:

`[-]PnYnMnWnD`

Where:

- `-` negates the whole period (optional).
- `P` indicates the literal is a period or duration.
- `nY` is the number of years (optional).
- `nM` is the number of months (optional).
- `nW` is the number of weeks, each converted to seven days (optional).
- `nD` is the number of days (optional).
- `n` is an integer, which can be negative.

The following code block creates a `Period` column with a value of 1 year, 4 months, and 5 days using a literal:

```groovy order=source
source = emptyTable(1).update("PeriodLiteral = 'P1Y4M5D'")
```

### ZoneId

A [`java.time.ZoneId`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZoneId.html) is a time zone. A time zone name, a Deephaven [time zone alias](../conceptual/time-in-deephaven.md#time-zones-and-aliases), or an offset in single quotes becomes a `ZoneId`, for example `'America/New_York'`, `'ET'`, or `'-05:00'`.

A one-character zone such as `'Z'` is a [character literal](./string-char-literals.md) instead, so write UTC as `'UTC'`.

The following code block creates three `ZoneId` columns: one from a time zone name, one from an alias, and one from an offset. The `sourceMeta` table shows that all three columns are `ZoneId` columns:

```groovy order=source,sourceMeta
source = emptyTable(1).update(
    "TimeZoneLiteral = 'America/New_York'",
    "TimeZoneAlias = 'ET'",
    "TimeZoneOffset = '-05:00'",
)
sourceMeta = source.meta()
```

## Related documentation

- [Work with time](../conceptual/time-in-deephaven.md)
- [Query string overview](./query-string-overview.md)
- [Formulas](./formulas.md)
- [Set date-time format](./set-date-time-format.md)
- [Boolean and numeric literals](./boolean-numeric-literals.md)
- [String and char literals](./string-char-literals.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`meta`](../reference/table-operations/metadata/meta.md)
- [`update`](../reference/table-operations/select/update.md)
- [DateTime](../reference/query-language/types/date-time.md)
- [Durations](../reference/query-language/types/durations.md)
- [Periods](../reference/query-language/types/periods.md)
- [`emptyTable` Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#emptyTable(long))
- [`Table` Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
