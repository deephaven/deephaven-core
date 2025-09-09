---
title: microsOfMilli
---

`microsOfMilli` returns the number of microseconds that have elapsed since the top of the millisecond.

## Syntax

```
microsOfMilli(instant)
microsOfMilli(dateTime)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The [date-time](../../query-language/types/date-time.md) from which to return the elapsed time.

</Param>
<Param name="dateTime" type="ZonedDateTime">

The [date-time](../../query-language/types/date-time.md) from which to return the elapsed time.

</Param>
</ParamTable>

## Returns

The number of microseconds that have elapsed since the start of the millisecond represented by the provided [date-time](../../query-language/types/date-time.md).

> [!NOTE]
> Null input values will return `NULL_LONG`.
> Nanoseconds are rounded, not dropped; e.g., '20:41:39.123456700' has 457 micros, not 456.

## Examples

```groovy order=:log
datetime = parseInstant("2023-09-09T12:34:56.123456 ET")

micros = microsOfMilli(datetime)

println micros
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#microsOfMilli(java.time.ZonedDateTime))
