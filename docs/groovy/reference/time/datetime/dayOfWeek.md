---
title: dayOfWeek
---

`dayOfWeek` returns the integer value of the day of the week for an input date-time in the specified time zone, with 1 being Monday and 7 being Sunday.

## Syntax

```
dayOfWeek(instant, timeZone)
dayOfWeek(dateTime)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The instant for which to find the day of the month.

</Param>
<Param name="timeZone" type="TimeZone">

The time zone to use when interpreting the [date-time](../../query-language/types/date-time.md).

</Param>
<Param name="dateTime" type="ZonedDateTime">

The zoned date-time for which to find the day of the week.

</Param>
</ParamTable>

## Returns

Returns a 1-based int value of the day of the week.

## Examples

```groovy order=:log
datetime = parseInstant("2024-02-29T01:23:45 ET")
datetime_zoned = toZonedDateTime(datetime, timeZone("MT"))

day = dayOfWeek(datetime, timeZone("ET"))
day_zoned = dayOfWeek(datetime_zoned)

println day
println day_zoned
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [`toZonedDateTime`](./toZonedDateTime.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#dayOfWeek(java.time.Instant,java.time.ZoneId))
