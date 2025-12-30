---
title: microsToSeconds
---

`microsToSeconds` returns the number of seconds equivalent to the specified microseconds value.

## Syntax

```
microsToSeconds(micros)
```

## Parameters

<ParamTable>
<Param name="micros" type="long">

The amount of microseconds to convert to seconds.

</Param>
</ParamTable>

## Returns

The number of seconds equivalent to the specified microseconds value. Null input values will return `NULL_LONG`.

## Examples

```groovy order=:log
micros = 5000
println micros

println microsToSeconds(micros)
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#microsToSeconds(long))
