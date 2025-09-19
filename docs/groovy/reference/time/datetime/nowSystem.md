---
title: nowSystem
---

`nowSystem` provides the current date-time as an Instant, with nanosecond resolution according to the system clock.

## Syntax

```
nowSystem()
```

## Parameters

This method takes no arguments.

## Returns

The current date-time.

## Examples

```groovy order=:log
println nowSystem()
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#nowSystem())
