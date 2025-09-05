---
title: nowMillisResolution
---

`nowMillisResolution` provides the current date-time as an Instant, with millisecond resolution according to the current [clock].

## Syntax

```
nowMillisResolution()
```

## Parameters

This method takes no arguments.

## Returns

The current date-time.

## Examples

```groovy order=:log
println nowMillisResolution()
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#nowMillisResolution())
