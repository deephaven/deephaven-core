---
title: isBefore
---

`isBefore` compares two [date-time](../../query-language/types/date-time.md)s and returns `true` if the first parameter comes before the second parameter.

## Syntax

```
isBefore(instant1, instant2)
isBefore(dateTime1, dateTime2)
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

- `true` if the first [date-time](../../query-language/types/date-time.md) comes before the second date-time.
- `false` otherwise.

## Example

The following example shows how to use `isBefore` to compare two Instants.

```groovy order=:log
d1 = parseInstant("2022-01-01T00:00:00 ET")
d2 = parseInstant("2022-01-02T00:00:00 ET")

println isBefore(d1, d2)
println isBefore(d2, d1)
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseInstant`](./parseInstant.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#isBefore(java.time.ZonedDateTime,java.time.ZonedDateTime))
