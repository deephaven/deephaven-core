---
title: epochAutoToEpochNanos
---

`epochAutoToEpochNanos` converts an offset from the Epoch to a nanoseconds value.

## Syntax

```
epochAutoToEpochNanos(epochOffset)
```

## Parameters

<ParamTable>
<Param name="epochOffset" type="long">

The time offset from the Epoch. Can be in milliseconds, microseconds, or nanoseconds. Expected date ranges are used to infer the units for the offset.

</Param>
</ParamTable>

## Returns

The nanoseconds since the Epoch.

## Examples

```groovy order=:log
nanos = epochAutoToEpochNanos(1672594496)
println nanos
```

```groovy order=:log
nanos = epochAutoToEpochNanos(1672594496000)
println nanos
```

```groovy order=:log
nanos = epochAutoToEpochNanos(1672594496000000)
println nanos
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochAutoToEpochNanos(long))
