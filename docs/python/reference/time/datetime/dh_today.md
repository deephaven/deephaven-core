---
title: dh_today
---

`dh_today` provides the current date string according to the current clock.

> [!NOTE]
> Under most circumstances, this method will return the date according to current system time, but during replay simulations, this method can return the date according to replay time.

> [!IMPORTANT]
> In query strings, users should choose the built-in function [`today`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#today()) instead of `dh_today`. For more information, see:
>
> - [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
> - [Java/Python boundary crossings](../../../conceptual/python-java-boundary.md)
> - [Auto-imported functions](../../query-language/query-library/auto-imported-functions.md)

## Syntax

```python syntax
dh_today() -> str
```

## Returns

A string representation of the current date-time.

## Examples

```python
from deephaven.time import dh_today

today = dh_today()

print(today)
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.dh_today)
