---
title: toZonedDateTime
---

`toZonedDateTime` converts the supplied variables into a ZonedDateTime.

## Syntax

```
toInstant(instant, timeZone)
toInstant(date, time, timeZone)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The Instant to convert.

</Param>
<Param name="date" type="LocalDate">

The local date.

</Param>
<Param name="time" type="LocalTime">

The local time.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone.

</Param>
</ParamTable>

## Returns

A ZonedDateTime.

## Examples

```groovy order=:log
date = parseLocalDate(today())
time = parseLocalTime("11:11:11")

tz = timeZone("ET")

zdt = toZonedDateTime(date, time, tz)

println zdt
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseLocalDate`](./parseLocalDate.md)
- [`timeZone`](./timeZone.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#toZonedDateTime(java.time.Instant,java.time.ZoneId))
