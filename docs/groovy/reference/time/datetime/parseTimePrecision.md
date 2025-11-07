---
title: parseTimePrecision
---

The `parseTimePrecision` method returns an `Enum` indicating the level of precision in a time, date-time, or period nanos string. See [ChronoField](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/temporal/ChronoField.html) for a full list of Enums.

## Syntax

```
parseTimePrecision(s)
```

## Parameters

<ParamTable>
<Param name="s" type="string">

The time or date-time to evaluate.

</Param>
</ParamTable>

## Returns

An enum indicating the evaluated string's level of precision.

## Examples

```groovy order=:log
time1 = parseTimePrecision("1")

time2 = parseTimePrecision("10:00:00")

time3 = parseTimePrecision("10:00:00.00")

println time1

println time2

println time3
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseTimePrecision(java.lang.String))
