---
title: parseDuration
---

`parseDuration` parses a string argument as a [Duration](../../query-language/types/durations.md), which is a unit of time in terms of wall clock time (days, hours, minutes, seconds, and nanoseconds).

Duration strings are formatted according to the ISO-8601 duration format as `[-]PnDTnHnMn.nS`, where the coefficients can be positive or negative. Zero coefficients can be omitted. Optionally, the string can begin with a negative sign.

Throws an exception if the string cannot be parsed.

## Syntax

```
parseDuration(s)
```

## Parameters

<ParamTable>
<Param name="s" type="string">

The string to be converted.

Time duration strings can be formatted as `[-]PT[-]hh:mm:[ss.nnnnnnnnn]` or as a duration string formatted as `[-]PnDTnHnMn.nS`.yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ

</Param>
</ParamTable>

## Returns

A Duration.

## Examples

```groovy order=:log
one_day = parseDuration("P1D")
one_hour = parseDuration("PT1H")
println one_day
println one_hour
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Durations](../../query-language/types/durations.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseDuration(java.lang.String))
