---
title: secondOfMinute
---

`secondOfMinute` returns the number of seconds that have elapsed since the top of the minute for the specified [date-time](../../query-language/types/date-time.md).

## Syntax

```
secondOfMinute(instant, timeZone)
secondOfMinute(dateTime)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The [date-time](../../query-language/types/date-time.md) from which to return the number of seconds.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone.

</Param>
<Param name="dateTime" type="ZonedDateTime">

The [date-time](../../query-language/types/date-time.md) from which to return the number of seconds.

</Param>
</ParamTable>

## Returns

The specified [date-time](../../query-language/types/date-time.md) converted into seconds. Null input values will return `NULL_LONG`.

## Examples

```groovy order=:log
datetime = parseInstant("2022-03-01T12:34:56 ET")

second = secondOfMinute(datetime, timeZone("ET"))

println second
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#secondOfMinute(java.time.Instant,java.time.ZoneId))
