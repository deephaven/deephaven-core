---
title: dh_time_zone
---

`dh_time_zone` returns the current Deephaven system time zone.

> [!IMPORTANT]
> In query strings, users should choose the built-in function [`timeZone`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#timeZone(java.lang.String)) instead of `dh_time_zone`. For more information, see:
>
> - [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
> - [Java/Python boundary crossings](../../../conceptual/python-java-boundary.md)
> - [Auto-imported functions](../../query-language/query-library/auto-imported-functions.md)

## Syntax

```python syntax
time_zone() -> TimeZone
```

## Parameters

This function takes no parameters.

## Returns

Returns a Java TimeZone.

## Examples

```python order=:log
from deephaven.time import dh_time_zone

tz = dh_time_zone()

print(tz)
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.dh_time_zone)
