---
title: epochSecondsToZonedDateTime
---

`epochSecondsToZonedDateTime` converts an offset from the Epoch to a ZonedDateTime.

## Syntax

```
epochSecondsToZonedDateTime(seconds, timeZone)
```

## Parameters

<ParamTable>
<Param name="seconds" type="long">

The seconds from the Epoch.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone.

</Param>
</ParamTable>

## Returns

A ZonedDateTime.

## Examples

```groovy order=:log
datetime = epochSecondsToZonedDateTime(1641013200000, timeZone("ET"))
println datetime
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochSecondsToZonedDateTime(long,java.time.ZoneId))
