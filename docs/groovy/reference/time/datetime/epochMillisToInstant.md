---
title: epochMillisToInstant
---

`epochMillisToInstant` converts a value of milliseconds from Epoch to a [date-time](../../query-language/types/date-time.md).

## Syntax

```
epochMillisToInstant(millis)
```

## Parameters

<ParamTable>
<Param name="millis" type="long">

The milliseconds since Epoch.

</Param>
</ParamTable>

## Returns

A [date-time](../../query-language/types/date-time.md) representing milliseconds since Epoch.

## Examples

```groovy order=:log
datetime = epochMillisToInstant(1641013200000)
println datetime
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochMillisToInstant(long))
