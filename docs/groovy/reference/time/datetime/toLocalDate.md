---
title: toLocalDate
---

The `toLocalDate` method converts the supplied variables into a [LocalDate](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html).

A `LocalDate` is a Java object that represents a date with no time zone - for example, "1995-05-23".

## Syntax

```
toLocalDate(instant, timeZone)
toLocalDate(dateTime)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The Instant to convert.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone.

</Param>
<Param name="dateTime" type="ZonedDateTime">

The zoned date-time to convert.

</Param>
</ParamTable>

## Returns

A [LocalDate](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html).

## Examples

```groovy order=:log
println toLocalDate(now(), timeZone("UTC"))
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`timeZone`](./timeZone.md)
- [LocalDate](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#toLocalDate(java.time.Instant,java.time.ZoneId))
