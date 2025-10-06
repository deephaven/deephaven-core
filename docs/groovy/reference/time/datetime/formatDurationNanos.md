---
title: formatDurationNanos
---

`formatDurationNanos` returns a nanosecond duration formatted as a `[-]PThhh:mm:ss.nnnnnnnnn` string.

## Syntax

```
formatDurationNanos(nanos)
```

## Parameters

<ParamTable>
<Param name="nanos" type="long">

The duration in nanoseconds.

</Param>
</ParamTable>

## Returns

A duration, formatted as a `[-]PThhh:mm:ss.nnnnnnnnn` string.

## Examples

```groovy order=:log
nanos = 3456000

println formatDurationNanos(nanos)
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Durations](../../query-language/types/durations.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#formatDurationNanos(long))
