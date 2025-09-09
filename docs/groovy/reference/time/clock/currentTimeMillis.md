---
title: currentTimeMillis
---

`currentTimeMillis` returns the number of milliseconds since the epoch (1970-01-01T00:00:00Z).

## Syntax

```
currentTimeMillis()
```

## Parameters

This method takes no arguments.

## Returns

The number of milliseconds since the epoch (1970-01-01T00:00:00Z).

## Examples

```groovy order=:log
println currentClock().currentTimeMillis()
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/base/clock/Clock.html#currentTimeMillis())
