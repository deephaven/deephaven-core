---
title: monthOfYear
---

`monthOfYear` returns the 1-based value for the month of the year for a specified [date-time](../../query-language/types/date-time.md).

## Syntax

```
monthOfYear(instant, timeZone)
monthOfYear(dateTime)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The [date-time](../../query-language/types/date-time.md) from which to return the number of months.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone.

</Param>
<Param name="dateTime" type="ZonedDateTime">

The [date-time](../../query-language/types/date-time.md) from which to return the number of months.

</Param>
</ParamTable>

## Returns

Returns an int value representing the month of the year.

## Examples

```groovy order=:log
datetime = parseInstant("2022-01-01T12:34:56 ET")

month = monthOfYear(datetime, timeZone("ET"))

println month
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [`timeZone`](./timeZone.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#monthOfYear(java.time.Instant,java.time.ZoneId))
