---
title: secondsToMicros
---

`secondsToMicros` returns the number of microseconds equivalent to the specified `seconds` value.

## Syntax

```
secondsToMicros(seconds)
```

## Parameters

<ParamTable>
<Param name="seconds" type="long">

The amount of seconds to convert to microseconds.

</Param>
</ParamTable>

## Returns

The equivalent number of microseconds to the specified `seconds` value. Null input values will return `NULL_LONG`.

## Examples

```groovy order=:log
seconds = 132

println secondsToMicros(seconds)
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#secondsToMicros(long))
