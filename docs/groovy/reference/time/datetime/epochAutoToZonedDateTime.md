---
title: epochAutoToZonedDateTime
---

`epochAutoToZonedDateTime` converts an offset from the Epoch to a ZonedDateTime.

## Syntax

```
epochAutoToZonedDateTime(epochOffset, timeZone)
```

## Parameters

<ParamTable>
<Param name="epochOffset" type="long">

The time offset from the Epoch. Can be in milliseconds, microseconds, or nanoseconds. Expected date ranges are used to infer the units for the offset.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone.

</Param>
</ParamTable>

## Returns

A ZonedDateTime.

## Examples

```groovy order=null
offset = 164000
tz = timeZone("UTC")

zdt = epochAutoToZonedDateTime(offset, tz)
print(zdt)
```

```groovy order=null
offset = 164000000
tz = timeZone("UTC")

zdt = epochAutoToZonedDateTime(offset, tz)
print(zdt)
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`timeZone`](./timeZone.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochAutoToZonedDateTime(long,java.time.ZoneId))
