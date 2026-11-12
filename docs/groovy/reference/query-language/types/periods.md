---
title: Periods
---

Periods are a special type of string used to represent a period of calendar time, such as days, weeks, months, or years.

## Syntax

`[-]PnYnMnWnD`

- `[-]` - An optional sign to indicate that the period is negative. Omitting this makes the period positive.
- `P` - The prefix indicating this is a period string.
- `n` - An integer value.
- `Y` - Years
- `M` - Months
- `W` - Weeks
- `D` - Days

Each `n[Y|M|W|D]` value translates to a part of the time period. A valid period string can contain nearly any combination of these values. For example, `P1M1D` (1 month and 1 day), `P1Y3M` (1 year and 3 months), and `P3W2D` (3 weeks and 2 days) are all valid period strings.

A period has no hour, minute, or second parts. For a length of time measured in hours, minutes, or seconds, use a [duration](./durations.md).

## Example

The following example uses [`parsePeriod`](../../time/datetime/parsePeriod.md) to convert period strings to period objects.

```groovy
oneYear = parsePeriod("P1Y")
oneYearFiveMonths = parsePeriod("P1Y5M")
threeWeeksTwoDays = parsePeriod("P3W2D")
```

The following example shows how to use periods to add to [date-times](./date-time.md).

```groovy
dateTime = parseInstant("2020-01-01T00:00:00 ET")
periodPositive = parsePeriod("P4D")
periodNegative = parsePeriod("P-5D")

println plus(dateTime, periodPositive)
println plus(dateTime, periodNegative)
```

A period added to an `Instant` can contain only days and weeks. To add months or years, add the period to a `ZonedDateTime`.

## Related Documentation

- [How to use Deephaven's built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [Query language functions](../query-library/query-language-function-reference.md)
- [Javadoc](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html)
