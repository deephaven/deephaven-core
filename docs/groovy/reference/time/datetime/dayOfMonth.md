---
title: dayOfMonth
---

`dayOfMonth` returns the integer value of the day of the month for an input [date-time](../../query-language/types/date-time.md) and specified time zone.

## Syntax

```
dayOfMonth(instant, timeZone)
dayOfMonth(dateTime)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The Instant for which to find the day of the month.

</Param>
<Param name="timeZone" type="TimeZone">

The time zone to use when interpreting the [date-time](../../query-language/types/date-time.md).

</Param>
<Param name="dateTime" type="ZonedDateTime">

The zoned date-time for which to find the day of the month.

</Param>
</ParamTable>

## Returns

Returns a 1-based int value of the day of the month for an Instant or a ZonedDateTime.

## Examples

```groovy order=:log
datetime = parseInstant("2024-02-29T01:23:45 ET")
datetime_zoned = toZonedDateTime(datetime, timeZone("MT"))

println dayOfMonth(datetime, timeZone("ET"))
println dayOfMonth(datetime_zoned)
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [`toZonedDateTime`](./toZonedDateTime.md)
- [`timeZone`](./timeZone.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#dayOfMonth(java.time.Instant,java.time.ZoneId))
