---
title: epochNanosToZonedDateTime
---

`epochNanosToZonedDateTime` converts an offset from the Epoch to a ZonedDateTime.

## Syntax

```
epochNanosToZonedDateTime(nanos, timeZone)
```

## Parameters

<ParamTable>
<Param name="nanos" type="long">

The nanoseconds from the Epoch.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone.

</Param>
</ParamTable>

## Returns

A ZonedDateTime.

## Examples

```groovy order=:log
datetime = epochNanosToZonedDateTime(1641013200000, timeZone("ET"))
println datetime
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochNanosToZonedDateTime(long,java.time.ZoneId))
