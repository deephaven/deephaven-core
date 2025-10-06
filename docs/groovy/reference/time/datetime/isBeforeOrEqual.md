---
title: isBeforeOrEqual
---

`isBeforeOrEqual` compares two date-times and returns `true` if the first parameter is equal to or earlier than the second parameter.

## Syntax

```
isBefore(instant1, instant2)
isBefore(dateTime1, dateTime2)
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

- `true` if the first date-time comes before the second date-time, or if the date-times are equal.
- `false` otherwise.

## Example

The following example shows how to use `isBeforeOrEqual` to compare three Instants.

```groovy order=:log
d1 = parseInstant("2022-01-01T00:00:00 ET")
d2 = parseInstant("2022-01-02T00:00:00 ET")
d3 = parseInstant("2022-01-02T00:00:00 ET")

println isBeforeOrEqual(d1, d2)
println isBeforeOrEqual(d2, d1)
println isBeforeOrEqual(d3, d2)
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#isBeforeOrEqual(java.time.Instant,java.time.Instant))
