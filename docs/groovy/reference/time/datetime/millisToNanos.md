---
title: millisToNanos
---

`millisToNanos` returns the equivalent number of nanoseconds from a given millisecond value.

## Syntax

```
millisToNanos(millis)
```

## Parameters

<ParamTable>
<Param name="millis" type="long">

The amount of milliseconds to convert to nanoseconds.

</Param>
</ParamTable>

## Returns

The equivalent number of nanoseconds as the specified milliseconds. Null input values will return `NULL_LONG`.

## Examples

```groovy order=null
nanos = millisToNanos(1641013200000)
println nanos
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#millisToNanos(long))
