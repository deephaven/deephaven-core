---
title: toExcelTime
---

The `toExcelTime` method converts a date-time to an Excel time represented as a double.

## Syntax

```
toExcelTime(instant, timeZone)
toExcelTime(dateTime)
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

An Excel time.

## Examples

```groovy order=:log
println toExcelTime(now(), timeZone("UTC"))
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`timeZone`](./timeZone.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#toLocalTime(java.time.Instant,java.time.ZoneId))
