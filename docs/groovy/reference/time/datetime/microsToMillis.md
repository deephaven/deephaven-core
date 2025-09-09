---
title: microsToMillis
---

`microsToMillis` returns the number of milliseconds equivalent to the specified microseconds value.

## Syntax

```
microsToMillis(micros)
```

## Parameters

<ParamTable>
<Param name="micros" type="long">

The amount of microseconds to convert to milliseconds.

</Param>
</ParamTable>

## Returns

The number of milliseconds equivalent to the specified microseconds value. Null input values will return `NULL_LONG`.

## Examples

```groovy order=:log
micros = 5000
println micros

println microsToMillis(micros)
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#microsToMillis(long))
