---
title: epochNanos
---

Returns nanoseconds from the Epoch for a date-time value.

## Syntax

```
epochNanos(instant)
epochNanos(dateTime)
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

Nanoseconds from the Epoch.

## Examples

```groovy order=:log
datetime = parseInstant("2022-01-01T05:04:02 ET")
println epochNanos(datetime)
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochNanos(java.time.Instant))
