---
title: isAfterOrEqual
---

`isAfterOrEqual` compares two date-times and returns `true` if the first parameter is equal to or later than the second parameter.

## Syntax

```
isAfter(instant1, instant2)
isAfter(dateTime1, dateTime2)
```

## Parameters

<ParamTable>
<Param name="instant1" type="Instant">

The first date-time for the comparison.

</Param>
<Param name="instant2" type="Instant">

The second date-time for the comparison.

</Param>
<Param name="dateTime1" type="ZonedDateTime">

The first date-time for the comparison.

</Param>
<Param name="dateTime2" type="dateTime2">

The second date-time for the comparison.

</Param>
</ParamTable>

## Returns

Returns a boolean value:

- `true` if the first date-time comes after the second date-time, or if the date-times are equal.
- `false` otherwise.

## Example

The following example shows how to use `isAfterOrEqual` to compare three Instants.

```groovy order=:log
d1 = parseInstant("2022-01-01T00:00:00 ET")
d2 = parseInstant("2022-01-02T00:00:00 ET")
d3 = parseInstant("2022-01-02T00:00:00 ET")

println isAfterOrEqual(d1, d2)
println isAfterOrEqual(d2, d1)
println isAfterOrEqual(d3, d2)
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#isAfterOrEqual(java.time.Instant,java.time.Instant))
