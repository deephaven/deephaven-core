---
title: toInstant
---

`toInstant` converts the supplied variables into an instant.

## Syntax

```
toInstant(date, time, timeZone)
toInstant(dateTime)
```

## Parameters

<ParamTable>
<Param name="date" type="LocalDate">

The local date.

</Param>
<Param name="time" type="LocalTime">

The local time.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone.

</Param>
<Param name="dateTime" type="ZonedDateTime">

The zoned date-time to convert.

</Param>
</ParamTable>

## Returns

An [Instant](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html).

## Examples

```groovy order=:log
date = parseLocalDate(today())
time = parseLocalTime("11:11:11")

tz = timeZone("ET")

instant = toInstant(date, time, tz)

println instant
```

## Related Documentation

- [Instant](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html)
- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseLocalDate`](./parseLocalDate.md)
- [`timeZone`](./timeZone.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#toInstant(java.time.ZonedDateTime))
