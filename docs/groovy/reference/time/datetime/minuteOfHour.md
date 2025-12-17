---
title: minuteOfHour
---

`minuteOfHour` returns the minutes since the top of the hour in the specified time zone.

## Syntax

```
minuteOfHour(instant, timeZone)
minuteOfHour(dateTime)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The [date-time](../../query-language/types/date-time.md) from which to return the number of minutes.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone.

</Param>
<Param name="dateTime" type="ZonedDateTime">

The [date-time](../../query-language/types/date-time.md) from which to return the number of minutes.

</Param>
</ParamTable>

## Returns

Returns an int value of minutes since midnight for a specified [date-time](../../query-language/types/date-time.md).

## Examples

```groovy order=:log
datetime = parseInstant("2022-01-01T12:34:56 ET")

minute = minuteOfHour(datetime, timeZone("ET"))

println minute
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [`timeZone](./timeZone.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#minuteOfHour(java.time.Instant,java.time.ZoneId))
