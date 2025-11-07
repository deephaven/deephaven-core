---
title: epochAutoToInstant
---

`epochAutoToInstant` converts an offset from the Epoch to an Instant.

## Syntax

```
epochAutoToInstant(epochOffset)
```

## Parameters

<ParamTable>
<Param name="epochOffset" type="long">

The time offset from the Epoch. Can be in milliseconds, microseconds, or nanoseconds. Expected date ranges are used to infer the units for the offset.

</Param>
</ParamTable>

## Returns

An Instant.

## Examples

```groovy order=:log
nanos = epochAutoToInstant(1672594496)
println nanos
```

```groovy order=:log
nanos = epochAutoToInstant(1672594496000)
println nanos
```

```groovy order=:log
nanos = epochAutoToInstant(1672594496000000)
println nanos
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochAutoToInstant(long))
