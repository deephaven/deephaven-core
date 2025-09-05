---
title: Durations
---

Durations are a special type of string used to represent a period of wall clock time (i.e. days, hours, minutes, seconds, nanoseconds).

## Syntax

`[-]PnDTnHnMn.nS`

- `[-]` - An optional sign to indicate that the period is negative. Omitting this makes the period positive.
- `P` - The prefix before any given number of days.
- `D` - Days
- `T` - The prefiex before hours, minutes, seconds, and nanoseconds.
- `n` - A numeric value
- `H` - Hours
- `M` - Minutes
- `S` - Seconds

Each `#[H|M|S]` value translates to a part of the duration. A valid duration string can contain nearly any combination of these values. For example, `PT1M1S` (1 minute and 1 second), `PT2H3M` (2 hours and 3 minutes), and `-PT24H30M2.4S` (negative 24 hours, 30 minutes, and 2.4 seconds) are all valid period strings.

## Example

The following example uses [`parseDuration`](../../time/datetime/parseDuration.md) to convert duration strings to duration objects.

```groovy order=:log
fiveHours = parseDuration("PT5H")
oneMinuteFifteenSeconds = parseDuration("PT1M15S")
negativeOneHour = parseDuration("PT-1H")
twoDaysTwoHours = parseDuration("P2DT2H")

println fiveHours
println oneMinuteFifteenSeconds
println negativeOneHour
println twoDaysTwoHours
```

The following example uses [`plus`](../../time/datetime/plus.md) to add Durations to an [Instant](../../query-language/types/date-time.md#instant).

```groovy order=:log
dateTime = parseInstant("2020-01-01T00:00:00 ET")
durationPositive = parseDuration("PT1H")
durationNegative = parseDuration("-PT1M1S")

println plus(dateTime, durationPositive)
println plus(dateTime, durationNegative)
```

## Related Documentation

- [How to use Deephaven's built-in query language functions](../../../how-to-guides/query-language-functions.md)
- [Query language functions](../query-library/query-language-function-reference.md)
- [`parseDuration`](../../time/datetime/parseDuration.md)
- [`plus`](../../time/datetime/plus.md)
- [date-time](./date-time.md)
- [Javadoc](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html)
