---
title: to_pd_timestamp
---

`to_pd_timestamp` converts a Java date-time to a `pandas.Timestamp`.

> [!WARNING]
> Avoid [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) functions in query strings. Because [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) provides functions that convert between Python and Java types, every call crosses the [Python-Java boundary](../../../conceptual/python-java-boundary.md), which slows performance. For more information, see:
>
> - [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
> - [Java/Python boundary crossings](../../../conceptual/python-java-boundary.md)

## Syntax

```python syntax
to_pd_timestamp(dt: Union[None, Instant, ZonedDateTime]) -> pandas.Timestamp
```

## Parameters

<ParamTable>
<Param name="dt" type="Union[None, Instant, ZonedDateTime]">

A Java date-time to convert to a `pandas.Timestamp`. If `None` is provided, `None` is returned.

</Param>
</ParamTable>

## Returns

Returns a `pandas.Timestamp`.

## Examples

```python order=null
from deephaven.time import dh_now, to_pd_timestamp

instant = dh_now()

pd_ts = to_pd_timestamp(instant)

print(pd_ts, type(pd_ts))
```

```python order=null
from deephaven.time import dh_now, to_pd_timestamp, to_j_zdt

zdt = to_j_zdt("2024-11-04T12:22:04 ET")

pd_ts = to_pd_timestamp(zdt)

print(pd_ts, type(pd_ts))
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timestamp)
