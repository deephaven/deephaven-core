---
title: millisOfSecond
---

`millisOfSecond` returns the number of milliseconds since the top of the second for a specified [date-time](../../query-language/types/date-time.md).

## Syntax

```
millisOfSecond(instant, timeZone)
millisOfSecond(dateTime)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The [date-time](../../query-language/types/date-time.md) from which to return the elapsed time.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone.

</Param>
<Param name="dateTime" type="ZonedDateTime">

The [date-time](../../query-language/types/date-time.md) from which to return the elapsed time.

</Param>
</ParamTable>

## Returns

Returns the number of milliseconds since the top of the second for the [date-time](../../query-language/types/date-time.md), or `NULL_INT` if dt is `None`.

## Examples

```groovy order=:log
datetime = parseInstant("2023-09-09T12:34:56.123456 ET")

millis = millisOfSecond(datetime, timeZone("ET"))

println millis
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [`timeZone`](./timeZone.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#millisOfSecond(java.time.Instant,java.time.ZoneId))
