---
title: currentTimeMicros
---

`currentTimeMicros` returns the number of microseconds since the epoch (1970-01-01T00:00:00Z).

The resolution is greater than or equal to `currentTimeMillis()`.

## Syntax

```
currentTimeMicros()
```

## Parameters

This method takes no arguments.

## Returns

The number of microseconds since the epoch (1970-01-01T00:00:00Z).

## Examples

```groovy order=:log
println currentClock().currentTimeNanos()
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/base/clock/Clock.html#currentTimeMicros())
