---
title: nanosToMillis
---

`nanosToMillis` returns the equivalent number of milliseconds from a given nanosecond value.

## Syntax

```
nanosToMillis(nanos)
```

## Parameters

<ParamTable>
<Param name="nanos" type="long">

The amount of nanoseconds to convert to milliseconds.

</Param>
</ParamTable>

## Returns

The equivalent number of milliseconds as the specified nanoseconds. Null input values will return `NULL_LONG`.

## Examples

```groovy order=:log
millis = nanosToMillis(1641013200000)
println millis
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#nanosToMillis(long))
