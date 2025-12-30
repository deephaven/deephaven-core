---
title: diffNanos
---

`diffNanos` returns the difference in nanoseconds between two [date-time](../../query-language/types/date-time.md) values, as an int.

## Syntax

```
diffNanos(start, end)
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

Returns the difference in nanoseconds between the supplied start and end values.

## Examples

The following example returns a positive value between two dates.

```groovy order=:log
d1 = parseInstant("2022-01-01T00:00:00 ET")
d2 = parseInstant("2022-01-02T00:00:00 ET")

difference = diffNanos(d1, d2)
println difference
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#diffNanos(java.time.Instant,java.time.Instant))
