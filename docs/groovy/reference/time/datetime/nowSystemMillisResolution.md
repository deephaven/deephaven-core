---
title: nowSystemMillisResolution
---

`nowSystemMillisResolution` provides the current date-time as an Instant, with millisecond resolution according to the System Clock.

## Syntax

```
nowSystemMillisResolution()
```

## Parameters

This method takes no arguments.

## Returns

The current date-time.

## Examples

```groovy order=:log
println nowSystemMillisResolution()
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#nowSystemMillisResolution())
