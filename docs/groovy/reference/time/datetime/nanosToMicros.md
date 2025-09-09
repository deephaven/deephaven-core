---
title: nanosToMicros
---

`nanosToMicros` returns the equivalent number of microseconds from a given nanosecond value.

## Syntax

```
nanosToMicros(nanos)
```

## Parameters

<ParamTable>
<Param name="nanos" type="long">

The amount of nanoseconds to convert to microseconds.

</Param>
</ParamTable>

## Returns

The equivalent number of microseconds as the specified nanoseconds. Null input values will return `NULL_LONG`.

## Examples

```groovy order=:log
nanos = nanosToMicros(1641013200000)
println nanos
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#nanosToMicros(long))
