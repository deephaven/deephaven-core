---
title: parseDurationQuiet
---

`parseDurationQuiet` parses a string argument as a [Duration](../../query-language/types/durations.md), which is a unit of time in terms of wall clock time (24-hour days, hours, minutes, seconds, and nanoseconds).

Duration strings are formatted according to the ISO-8601 duration format as `[-]PnDTnHnMn.nS`, where the coefficients can be positive or negative. Zero coefficients can be omitted. Optionally, the string can begin with a negative sign.

"Quiet" methods return `null` instead of throwing an exception when invalid input is given.

## Syntax

```
parseDurationQuiet(s)
```

## Parameters

<ParamTable>
<Param name="s" type="string">

The string to be converted.

Time duration strings can be formatted as `[-]PT[-]hh:mm:[ss.nnnnnnnnn]` or as a duration string formatted as `[-]PnDTnHnMn.nS`.

</Param>
</ParamTable>

## Returns

A Duration.

## Examples

```groovy order=:log
one_day = parseDurationQuiet("P1fD")
one_hour = parseDurationQuiet("PT1H")
println one_day
println one_hour
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Durations](../../query-language/types/durations.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseDurationQuiet(java.lang.String))
