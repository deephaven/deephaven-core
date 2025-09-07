---
title: parseInstant
---

`parseInstant` parses a string argument as an [Instant](../../query-language/types/date-time.md#instant), which represents a specific point in time. Resolution can be as precise as a millisecond.

Instant strings are formatted according to the ISO-8601 date-time format as `yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ`.

Throws an exception if the string cannot be parsed.

## Syntax

```
parseInstant(s)
```

## Parameters

<ParamTable>
<Param name="s" type="string">

The string to be converted.

Date-time strings can be formatted as `yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ`.

</Param>
</ParamTable>

## Returns

An Instant.

## Examples

```groovy order=:log
time = "1995-08-02 ET"

time2 = "2008-11-11T08:12:23.231453 ET"

println parseInstant(time)

println parseInstant(time2)
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Durations](../../query-language/types/durations.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseInstant(java.lang.String))
