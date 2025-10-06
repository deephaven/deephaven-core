---
title: parseTimeZoneQuiet
---

The `parseTimeZoneQuiet` method parses the supplied string as a time zone.

"Quiet" methods return `null` instead of throwing an exception when encountering a string that cannot be parsed.

## Syntax

```
parseTimeZoneQuiet(s)
```

## Parameters

<ParamTable>
<Param name="s" type="string">

The string to be converted.

</Param>
</ParamTable>

## Returns

A ZoneId, or `null` if invalid input is given.

## Examples

```groovy order=:log
tz = parseTimeZoneQuiet("America/Los_Angeles")

tz2 = parseTimeZoneQuiet("Amurica/Los_Angeles")

println epochMillisToZonedDateTime(100, tz)

println epochMillisToZonedDateTime(100, tz2)
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`epochMillisToZonedDateTime`](./epochMillisToZonedDateTime.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseTimeZoneQuiet(java.lang.String))
