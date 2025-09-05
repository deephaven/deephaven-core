---
title: dh_now
---

`dh_now` provides the current datetime as an [Instant](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html).

> [!IMPORTANT]
> In query strings, users should choose the built-in function [`now`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#now()) instead of `dh_now`. For more information, see:
>
> - [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
> - [Java/Python boundary crossings](../../../conceptual/python-java-boundary.md)
> - [Auto-imported functions](../../query-language/query-library/auto-imported-functions.md)

## Syntax

```python syntax
dh_now(
    system: bool = false,
    resolution: str = 'ns',
) -> Instant
```

## Parameters

<ParamTable>
<Param name="system" type="bool" optional>

`True` to use the system clock; `False` to use the default clock. Under most circumstances, the default clock will return the current system time, but during replay simulations, the default clock can return the replay time. The default is `False`.

</Param>
<Param name="resolution" type="str" optional>

The resolution of the returned time. The default ‘ns’ will return nanosecond resolution times if possible. ‘ms’ will return millisecond resolution times.

</Param>
</ParamTable>

## Returns

An Instant representation of the current time.

## Examples

```python
from deephaven.time import dh_now

print(dh_now())
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.dh_now)
