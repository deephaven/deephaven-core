---
title: parseEpochNanosQuiet
---

`parseEpochNanosQuiet` converts a string string argument to nanoseconds since the Epoch.
Date-time strings are formatted according to the ISO 8601 date time format `yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ` and others. Additionally, date time strings can be integer values that are nanoseconds, milliseconds, or seconds from the Epoch. Expected date ranges are used to infer the units.

"Quiet" methods return `null` instead of throwing an exception when encountering a string that cannot be parsed.

## Syntax

```
parseEpochNanosQuiet(s)
```

## Parameters

<ParamTable>
<Param name="s" type="string">

The string to be converted.

</Param>
</ParamTable>

## Returns

Nanoseconds since the Epoch, or `null` if invalid input is given.

## Examples

```groovy order=:log
parse1 = parseEpochNanosQuiet("2022-01-01T12:34:56 ET")
parse2 = parseEpochNanosQuiet("2012-01-01T12:34:56 ET")
println parse1
println parse2
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseEpochNanosQuiet(java.lang.String))
