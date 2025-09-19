---
title: parseDurationNanos
---

`parseDurationNanos` converts a string representing a [Duration](../../query-language/types/durations.md) to nanoseconds.

## Syntax

```
parseDurationNanos(s)
```

## Parameters

<ParamTable>
<Param name="s" type="string">

The string to be converted.

Time duration strings can be formatted as `[-]PT[-]hh:mm:[ss.nnnnnnnnn]` or as a Duration string formatted as `[-]PnDTnHnMn.nS`.

</Param>
</ParamTable>

## Returns

Nanoseconds representation of the time string.

## Examples

```groovy order=:log
one_day_nanos = parseDurationNanos("P1D")
one_hour_nanos = parseDurationNanos("PT1H")
println one_day_nanos
println one_hour_nanos
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Durations](../../query-language/types/durations.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseDurationNanos(java.lang.String))
