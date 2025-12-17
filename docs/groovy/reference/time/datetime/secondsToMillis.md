---
title: secondsToMillis
---

`secondsToMillis` returns the number of milliseconds equivalent to the specified `seconds` value.

## Syntax

```
secondsToMillis(seconds)
```

## Parameters

<ParamTable>
<Param name="seconds" type="long">

The amount of seconds to convert to milliseconds.

</Param>
</ParamTable>

## Returns

The equivalent number of milliseconds to the specified `seconds` value. Null input values will return `NULL_LONG`.

## Examples

```groovy order=:log
seconds = 132

println secondsToMillis(seconds)
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#secondsToMillis(long))
