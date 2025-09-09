---
title: dayOfYear
---

`dayOfYear` returns the integer value of the day of the year for an input [date-time](../../query-language/types/date-time.md) in the specified time zone.

## Syntax

```
dayOfYear(instant, timeZone)
dayOfYear(dateTime)
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

The zoned date-time for which to find the day of the year.

</Param>
</ParamTable>

## Returns

Returns a 1-based int value of the day of the year.

## Examples

```groovy
datetime = parseInstant("2024-02-29T01:23:45 ET")
datetime_zoned = toZonedDateTime(datetime, timeZone("MT"))

day = dayOfYear(datetime, timeZone("ET"))
day_zoned = dayOfYear(datetime_zoned)

println day
println day_zoned
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [`timeZone`](./timeZone.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#dayOfYear(java.time.ZonedDateTime))
