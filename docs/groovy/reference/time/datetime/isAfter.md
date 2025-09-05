---
title: isAfter
---

`isAfter` compares two [`date-times`](../../query-language/types/date-time.md) and returns `true` if the first parameter comes after the second parameter.

## Syntax

```
isAfter(instant1, instant2)
isAfter(dateTime1, dateTime2)
```

## Parameters

<ParamTable>
<Param name="instant1" type="Instant">

The first [date-time](../../query-language/types/date-time.md) for the comparison.

</Param>
<Param name="instant2" type="Instant">

The second [date-time](../../query-language/types/date-time.md) for the comparison.

</Param>
<Param name="dateTime1" type="ZonedDateTime">

The first [date-time](../../query-language/types/date-time.md) for the comparison.

</Param>
<Param name="dateTime2" type="dateTime2">

The second [date-time](../../query-language/types/date-time.md) for the comparison.

</Param>
</ParamTable>

## Returns

Returns a boolean value:

- `true` if the first [date-time](../../query-language/types/date-time.md) comes after the second date-time.
- `false` otherwise.

## Example

The following example shows how to use `isAfter` to compare two Instants.

```groovy order=:log
d1 = parseInstant("2022-01-01T00:00:00 ET")
d2 = parseInstant("2022-01-02T00:00:00 ET")

println isAfter(d1, d2)
println isAfter(d2, d1)
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#isAfter(java.time.ZonedDateTime,java.time.ZonedDateTime))
