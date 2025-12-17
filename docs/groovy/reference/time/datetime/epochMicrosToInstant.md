---
title: epochMicrosToInstant
---

`epochMicrosToInstant` converts a value of microseconds from Epoch to a date-time.

## Syntax

```
epochMicrosToInstant(micros)
```

## Parameters

<ParamTable>
<Param name="micros" type="long">

The microseconds since Epoch.

</Param>
</ParamTable>

## Returns

A date-time representing microseconds since Epoch.

## Examples

```groovy order=:log
datetime = epochMicrosToInstant(1641013200000)
println datetime
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochMicrosToInstant(long))
