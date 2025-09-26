---
title: Periods
---

Periods are a special type of string used to represent a period of time.

## Syntax

`-#Y#M#DT#H#M#S`

- `-` - An optional sign to indicate that the period is negative. Omitting this makes the period positive.
- `#` - Any numeric value
- `Y` - Years
- `M` (left of `T`) - Months
- `D` - Days
- `T` - A divider between the year/month/day and hour/minute/second times
- `H` - Hours
- `M` (right of `T`) - Minutes
- `S` - Seconds

Each `#[Y|M|D|H|M|S]` value translates to a part of the time period. A valid period string can contain nearly any combination of these values. For example, `1M1D` (1 month and 1 day), `1YT3S` (1 year and 3 seconds), and `T3H2M` (3 hours and 2 minutes) are all valid period strings.

The `T` separator is required only if the period string contains an hour, minute, or second time.

> [!NOTE]
> Some Deephaven methods use periods that can only be less than a day in length. In these cases, the `T` is still required in the period string.

## Example

The following example uses [parsePeriod] to convert period strings to period objects.

```groovy
oneYear = parsePeriod("P1Y")
oneYearFiveMonths = parsePeriod("P1Y5M")
fiveMinutesTenSeconds = parseDuration("PT5M1S")
```

The following example shows how to use periods to add to [date-times](./date-time.md).

```groovy
dateTime = parseInstant("2020-01-01T00:00:00 ET")
periodPositive = parsePeriod("P4D")
periodNegative = parsePeriod("P-5D")

println plus(dateTime, periodPositive)
println plus(dateTime, periodNegative)
```

## Related Documentation

- [How to use Deephaven's built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [Query language functions](../query-library/query-language-function-reference.md)
- [Javadoc](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html)
