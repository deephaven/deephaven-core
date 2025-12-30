---
title: epochMillisToZonedDateTime
---

`epochMillisToZonedDateTime` converts an offset from the Epoch to a ZonedDateTime.

## Syntax

```
epochMillisToZonedDateTime(millis, timeZone)
```

## Parameters

<ParamTable>
<Param name="millis" type="long">

The milliseconds from the Epoch.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone.

</Param>
</ParamTable>

## Returns

A ZonedDateTime.

## Examples

```groovy order=:log
datetime = epochMillisToZonedDateTime(1641013200000, timeZone("ET"))
println datetime
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`timeZone`](./timeZone.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochMillisToZonedDateTime(long,java.time.ZoneId))
