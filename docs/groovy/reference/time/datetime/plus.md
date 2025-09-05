---
title: plus
---

`plus` adds one time period to another.

## Syntax

```
plus(instant, nanos)
plus(instant, duration)
plus(instant1, instant2)
plus(instant, period)
plus(dateTime, nanos)
plus(dateTime, duration)
plus(dateTime1, dateTime2)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

Starting instant value.

</Param>
<Param name="instant1" type="Instant">

Starting instant value.

</Param>
<Param name="dateTime" type="ZonedDateTime">

Starting [date-time](../../query-language/types/date-time.md) value.

</Param>
<Param name="dateTime1" type="ZonedDateTime">

Starting [date-time](../../query-language/types/date-time.md) value.

</Param>
<Param name="nanos" type="long">

Number of nanoseconds to subtract.

</Param>
<Param name="duration" type="Duration">

The time period to subtract.

</Param>
<Param name="period" type="Period">

The time period to subtract.

</Param>
<Param name="instant2" type="Instant">

The time period to subtract.

</Param>
<Param name="dateTime2" type="ZonedDateTime">

The time period to subtract.

</Param>
</ParamTable>

## Returns

The starting time plus the specified time period.

## Examples

```groovy order=:log
dateTime = parseInstant("2020-01-02T00:00:00 ET")
duration = parseDuration("PT5H3M35S")

println plus(dateTime, duration)
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Durations](../../query-language/types/durations.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#plus(java.time.Instant,long))
