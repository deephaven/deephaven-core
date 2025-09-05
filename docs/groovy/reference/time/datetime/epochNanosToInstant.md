---
title: epochNanosToInstant
---

`epochNanosToInstant` converts a value of nanoseconds from Epoch to a date-time Instant.

## Syntax

```
epochNanosToInstant(nanos)
```

## Parameters

<ParamTable>
<Param name="nanos" type="long">

The nanoseconds since Epoch.

</Param>
</ParamTable>

## Returns

A date-time representing nanoseconds since Epoch.

## Examples

```groovy order=:log
datetime = epochNanosToInstant(1641013200000)
println datetime
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochNanosToInstant(long))
