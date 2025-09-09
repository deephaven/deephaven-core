---
title: epochSecondsToInstant
---

`epochSecondsToInstant` converts a value of seconds from Epoch to a date-time Instant.

## Syntax

```
epochSecondsToInstant(seconds)
```

## Parameters

<ParamTable>
<Param name="seconds" type="long">

The seconds since Epoch.

</Param>
</ParamTable>

## Returns

A date-time representing seconds since Epoch.

## Examples

```groovy order=:log
datetime = epochSecondsToInstant(1641013200000)
println datetime
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochSecondsToInstant(long))
