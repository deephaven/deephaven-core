---
title: nanosOfMilli
---

`nanosOfMilli` returns the number of nanoseconds that have elapsed since the top of the millisecond for the specified date-time.

## Syntax

```
nanosOfMilli(instant)
nanosOfMilli(dateTime)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The date-time from which to return the number of nanoseconds.

</Param>
<Param name="dateTime" type="ZonedDateTime">

The date-time from which to return the number of nanoseconds.

</Param>
</ParamTable>

## Returns

The specified date-time converted into nanoseconds. Null input values will return `NULL_LONG`.

## Examples

```groovy order=:log
datetime = parseInstant("2022-03-01T12:34:56.45555 ET")

nanos = nanosOfMilli(datetime)

println nanos
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#nanosOfMilli(java.time.Instant))
