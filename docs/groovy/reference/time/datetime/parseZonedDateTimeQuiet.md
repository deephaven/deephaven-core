---
title: parseZonedDateTimeQuiet
---

The `parseZonedDateTimeQuiet` method parses the supplied string as a time zone.
Date-time strings are formatted according to the ISO-8601 format `yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ` and others.

"Quiet" methods return `null` instead of throwing an exception when encountering a string that cannot be parsed.

## Syntax

```
parseZonedDateTimeQuiet(s)
```

## Parameters

<ParamTable>
<Param name="s" type="string">

The string to be converted.

</Param>
</ParamTable>

## Returns

A ZonedDateTime, or `null` if invalid input is given.

## Examples

```groovy order=:log
println parseZonedDateTimeQuiet("1982-09-23T11:23:00 ET")
println parseZonedDateTimeQuiet("198f2-09-23T11:23:00 ET")
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseZonedDateTimeQuiet(java.lang.String))
