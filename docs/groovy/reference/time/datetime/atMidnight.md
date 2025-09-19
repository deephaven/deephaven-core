---
title: atMidnight
---

`atMidnight` returns an instant for the requested input value at midnight in the specified time zone.

## Syntax

```
atMidnight(instant, timeZone)
atMidnight(dateTime)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The time to compute the prior midnight for.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone for which the new value at midnight should be calculated.

</Param>
<Param name="dateTime" type="ZonedDateTime">

The time to compute the prior midnight for.

</Param>
</ParamTable>

## Returns

Returns either an Instant or a ZonedDateTime for the prior midnight in the specified time zone.

## Examples

```groovy
datetime = parseInstant("2023-01-01T00:05:00 ET")
mt = timeZone("MT")
datetime_zoned = toZonedDateTime(datetime, mt)

println atMidnight(datetime, mt)
println atMidnight(datetime_zoned)
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [`timeZone`](./timeZone.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#atMidnight(java.time.Instant,java.time.ZoneId))
