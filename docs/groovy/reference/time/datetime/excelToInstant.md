---
title: excelToInstant
---

`excelToInstant` converts an Excel date-time to an Instant.

## Syntax

```
excelToInstant(excel, timeZone)
```

## Parameters

<ParamTable>
<Param name="excel" type="double">

Excel time, represented as a double.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone.

</Param>
</ParamTable>

## Returns

An Instant date-time.

## Examples

```groovy order=:log
datetime = excelToInstant(1641013200000, timeZone("ET"))
println datetime
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`timeZone`](./timeZone.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#excelToInstant(double,java.time.ZoneId))
