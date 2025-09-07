---
title: parseTimePrecisionQuiet
---

The `parseTimePrecisionQuiet` method returns an `Enum` indicating the level of precision in a time, date-time, or period nanos string. See [ChronoField](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/temporal/ChronoField.html) for a full list of Enums.

"Quiet" methods return `null` instead of throwing an exception when encountering a string that cannot be parsed.

## Syntax

```
parseTimePrecisionQuiet(s)
```

## Parameters

<ParamTable>
<Param name="s" type="string">

The time or date-time to evaluate.

</Param>
</ParamTable>

## Returns

An enum indicating the evaluated string's level of precision, or `null` if invalid input is given.

## Examples

```groovy order=:log
time1 = parseTimePrecisionQuiet("1:00")

time_invalid = parseTimePrecisionQuiet("10:00f:00")

println time1

println time_invalid
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseTimePrecisionQuiet(java.lang.String))
