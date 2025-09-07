---
title: diffYears365
---

`diffYears365` returns the difference in years (defined as 365 days) between two date-time values, as a `double`.

## Syntax

```
diffYears365(start, end)
```

## Parameters

<ParamTable>
<Param name="start" type="Instant">

The start time of the range.

</Param>
<Param name="start" type="ZonedDateTime">

The start time of the range.

</Param>
<Param name="end" type="Instant">

The end time of the range.

</Param>
<Param name="end" type="ZonedDateTime">

The end time of the range.

</Param>
</ParamTable>

## Returns

Returns the difference in years between the supplied start and end values.

## Examples

```groovy order=:log
d1 = parseInstant("2022-01-01T00:00:00 ET")
d2 = parseInstant("2023-01-02T00:00:00 ET")

difference = diffYears365(d1, d2)
println difference
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#diffYears365(java.time.Instant,java.time.Instant))
