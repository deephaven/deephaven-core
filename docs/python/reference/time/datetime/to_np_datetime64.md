---
title: to_np_datetime64
---

`to_np_datetime64` converts a Java date-time to a `numpy.datetime64`.

> [!WARNING]
> Avoid [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) functions in query strings. Because [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) provides functions that convert between Python and Java types, every call crosses the [Python-Java boundary](../../../conceptual/python-java-boundary.md), which slows performance. For more information, see:
>
> - [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
> - [Java/Python boundary crossings](../../../conceptual/python-java-boundary.md)

## Syntax

```python syntax
to_np_datetime64(dt: Union[None, Instant, ZonedDateTime]) -> numpy.datetime64
```

## Parameters

<ParamTable>
<Param name="dt" type="Union[None, Instant, ZonedDateTime]">

A Java date-time to convert to a `numpy.datetime64`. If `None` is provided, `None` is returned.

</Param>
</ParamTable>

## Returns

Returns a `numpy.datetime64`.

## Examples

```python order=:log
from deephaven.time import dh_now, to_np_datetime64

np_now = to_np_datetime64(dh_now())

print(np_now, type(np_now))
```

```python order=:log
from deephaven.time import dh_now, to_np_datetime64, to_j_zdt

zdt = to_j_zdt("2024-05-22 ET")

np_dt = to_np_datetime64(zdt)

print(np_dt)
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_datetime64)
