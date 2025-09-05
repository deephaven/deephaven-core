---
title: nanosOfSecond
---

`nanosOfSecond` returns the number of nanoseconds that have elapsed since the top of the second for the specified [date-time](../../query-language/types/date-time.md).

## Syntax

```
nanosOfSecond(dateTime, timeZone)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The [date-time](../../query-language/types/date-time.md) from which to return the number of nanoseconds.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone.

</Param>
<Param name="dateTime" type="ZonedDateTime">

The [date-time](../../query-language/types/date-time.md) from which to return the number of nanoseconds.

</Param>
</ParamTable>

## Returns

The specified [date-time](../../query-language/types/date-time.md) converted into nanoseconds. Null input values will return `NULL_LONG`.

## Examples

```groovy order=:log
datetime = parseInstant("2022-03-01T12:34:56 ET")

nanos = nanosOfSecond(datetime, timeZone("ET"))

println nanos
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [`timeZone`](./timeZone.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#nanosOfSecond(java.time.Instant,java.time.ZoneId))
