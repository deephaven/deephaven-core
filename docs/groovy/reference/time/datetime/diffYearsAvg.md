---
title: diffYearsAvg
---

`diffYearsAvg` returns the difference in years (defined as 365.2425 days to compensate for leap years) between two date-time values, as a `double`.

## Syntax

```
diffYearsAvg(start, end)
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

```groovy order=null
d1 = parseInstant("2022-01-01T00:00:00 ET")
d2 = parseInstant("2028-01-02T00:00:00 ET")

difference = diffYearsAvg(d1, d2)
println difference
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#diffYearsAvg(java.time.Instant,java.time.Instant))
