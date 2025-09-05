---
title: parseDurationNanosQuiet
---

`parseDurationNanosQuiet` converts a string representing a period or [Duration](../../query-language/types/durations.md) to nanoseconds. Time duration strings can be formatted as `[-]PT[-]hh:mm:[ss.nnnnnnnnn]` or as a duration string formatted as `[-]PnDTnHnMn.nS`.

"Quiet" methods return `null` instead of throwing an exception when encountering a string that cannot be parsed.

## Syntax

```
parseDurationNanosQuiet(s)
```

## Parameters

<ParamTable>
<Param name="s" type="string">

The string to be converted.

Time duration strings can be formatted as `[-]PT[-]hh:mm:[ss.nnnnnnnnn]` or as a duration string formatted as `[-]PnDTnHnMn.nS`.

</Param>
</ParamTable>

## Returns

Nanoseconds representation of the time string, or `null` if invalid input is given.

## Examples

```groovy order=:log
one_day_nanos = parseDurationNanosQuiet("P1D")
one_hour_nanos = parseDurationNanosQuiet("PT1H")
println one_day_nanos
println one_hour_nanos
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Durations](../../query-language/types/durations.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseDurationNanosQuiet(java.lang.String))
