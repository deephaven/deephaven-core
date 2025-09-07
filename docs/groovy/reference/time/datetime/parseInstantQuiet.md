---
title: parseInstantQuiet
---

`parseInstantQuiet` parses a string argument as an Instant, which represents a specific point in time. Resolution can be as precise as a millisecond.

Instant strings are formatted according to the ISO-8601 date-time format as `yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ`.

"Quiet" methods return `null` instead of throwing an exception when encountering a string that cannot be parsed.

## Syntax

```
parseInstantQuiet(s)
```

## Parameters

<ParamTable>
<Param name="s" type="string">

The string to be converted.

Date-time strings can be formatted as `yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ`.

</Param>
</ParamTable>

## Returns

An Instant, or `null` if invalid input is given.

## Examples

```groovy order=:log
time = "1995-08-02 ET"

time2 = "2008-11-11T08:12:23.231453 ET"

time3 = "invalid time"

println parseInstantQuiet(time)

println parseInstantQuiet(time2)

println parseInstantQuiet(time3)
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Durations](../../query-language/types/durations.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseInstantQuiet(java.lang.String))
