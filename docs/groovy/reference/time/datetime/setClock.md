---
title: setClock
---

`setClock` sets the clock used to compute the current time. This allows a custom clock to be used instead of the current system clock. This is mainly used for replay simulations.

## Syntax

```
setClock(clock)
```

## Parameters

<ParamTable>
<Param name="clock" type="Clock">

The clock to use.

</Param>
</ParamTable>

## Returns

Sets the clock.

## Examples

The following example sets the clock to the current clock used by the system.

```groovy order=null
setClock(currentClock())
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [currentClock](../datetime/currentClock.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#setClock(io.deephaven.base.clock.Clock))
