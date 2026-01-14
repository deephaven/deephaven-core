---
title: epochMicrosToZonedDateTime
---

`epochMicrosToZonedDateTime` converts an offset from the Epoch to a ZonedDateTime.

## Syntax

```
epochMicrosToZonedDateTime(micros, timeZone)
```

## Parameters

<ParamTable>
<Param name="micros" type="long">

The microseconds from the Epoch.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone.

</Param>
</ParamTable>

## Returns

A ZonedDateTime.

## Examples

```groovy order=:log
datetime = epochMicrosToZonedDateTime(1641013200000, timeZone("ET"))
println datetime
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochMicrosToZonedDateTime(long,java.time.ZoneId))
