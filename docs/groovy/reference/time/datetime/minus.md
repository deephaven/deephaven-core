---
title: minus
---

`minus` subtracts one time period from another.

## Syntax

```
minus(instant, nanos)
minus(instant, duration)
minus(instant1, instant2)
minus(instant, period)
minus(dateTime, nanos)
minus(dateTime, duration)
minus(dateTime1, dateTime2)
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

The starting time minus the specified time period.

## Examples

```groovy order=:log
d1 = parseInstant("2020-01-02T00:00:00 ET")
d2 = parseInstant("2020-01-01T00:00:00 ET")

println minus(d1, d2)
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#minus(java.time.Instant,long))
