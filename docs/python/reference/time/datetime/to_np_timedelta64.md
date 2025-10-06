---
title: to_np_timedelta64
---

`to_np_timedelta64` converts a Java time duration to a `numpy.timedelta64`.

> [!WARNING]
> Avoid [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) functions in query strings. Because [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) provides functions that convert between Python and Java types, every call crosses the [Python-Java boundary](../../../conceptual/python-java-boundary.md), which slows performance. For more information, see:
>
> - [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
> - [Java/Python boundary crossings](../../../conceptual/python-java-boundary.md)

## Syntax

```python syntax
to_np_timedelta64(dt: Union[None, Duration, Period]) -> numpy.timedelta64
```

## Parameters

<ParamTable>
<Param name="dt" type="Union[None, Duration, Period]">

A Java time duration to convert to a `numpy.timedelta64`. If `None` is provided, `None` is returned.

</Param>
</ParamTable>

## Returns

Returns a `numpy.timedelta64`.

## Examples

```python order=null
from deephaven.time import to_j_duration, to_np_timedelta64

duration = to_j_duration("PT42M")

np_td = to_np_timedelta64(duration)

print(np_td, type(np_td))
```

```python order=null
from deephaven.time import dh_now, to_np_timedelta64, to_j_period, to_j_duration

period = to_j_period("P2Y")

np_td = to_np_timedelta64(period)

print(np_td)
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_timedelta64)
