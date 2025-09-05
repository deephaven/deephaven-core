---
title: parseTimeZone
---

The `parseTimeZone` method parses the supplied string as a time zone.

## Syntax

```
parseTimeZone(s)
```

## Parameters

<ParamTable>
<Param name="s" type="string">

The string to be converted.

</Param>
</ParamTable>

## Returns

A ZoneId.

## Examples

```groovy order=:log
tz = parseTimeZone("America/Los_Angeles")

println epochMillisToZonedDateTime(100, tz)
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`epochMillisToZonedDateTime`](./epochMillisToZonedDateTime.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseTimeZone(java.lang.String))
