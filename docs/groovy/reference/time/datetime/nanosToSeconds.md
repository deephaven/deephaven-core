---
title: nanosToSeconds
---

`nanosToSeconds` returns the number of seconds equivalent to the specified nanoseconds value.

## Syntax

```
nanosToSeconds(nanos)
```

## Parameters

<ParamTable>
<Param name="nanos" type="long">

The amount of nanoseconds to convert to seconds.

</Param>
</ParamTable>

## Returns

The number of seconds equivalent to the specified nanoseconds value. Null input values will return `NULL_LONG`.

## Examples

```groovy order=:log
nanos = 5000
println nanos

println nanosToSeconds(nanos)
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#nanosToSeconds(long))
