---
title: formatDate
---

`formatDate` returns a [string](../../query-language/types/strings.md) representation of a [date-time](../../query-language/types/date-time.md) for a specified time zone, formatted as `yyyy-MM-dd`.

## Syntax

```
formatDate(instant, timeZone)
formatDate(dateTime)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

The Instant to format as a string.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone used when formatting the string.

</Param>
<Param name="dateTime" type="ZonedDateTime">

The time to format as a string.

</Param>
</ParamTable>

## Returns

The time, formatted as a `yyyy-MM-dd` string.

## Examples

```groovy order=null
datetime = parseInstant("2022-01-01T00:00:00 ET")

formatted_date = formatDate(datetime, timeZone("ET"))
println formatted_date
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [`timeZone`](./timeZone.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#formatDate(java.time.Instant,java.time.ZoneId))
