---
title: epochMillis
---

Returns milliseconds from the Epoch for a date-time value.

## Syntax

```
epochMillis(instant)
epochMillis(dateTime)
```

## Parameters

<ParamTable>
<Param name="instant" type="Instant">

Instant to compute the Epoch offset for.

</Param>
<Param name="dateTime" type="ZonedDateTime">

ZonedDateTime to compute the Epoch offset for.

</Param>
</ParamTable>

## Returns

Milliseconds from the Epoch.

## Examples

```groovy order=:log
datetime = parseInstant("2022-01-01T05:04:02 ET")
println epochMillis(datetime)
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochMillis(java.time.Instant))
