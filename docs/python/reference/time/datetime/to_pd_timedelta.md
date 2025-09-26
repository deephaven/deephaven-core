---
title: to_pd_timedelta
---

`to_pd_timedelta` converts a Java Duration to a `pandas.Timedelta`.

> [!WARNING]
> Avoid [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) functions in query strings. Because [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) provides functions that convert between Python and Java types, every call crosses the [Python-Java boundary](../../../conceptual/python-java-boundary.md), which slows performance. For more information, see:
>
> - [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
> - [Java/Python boundary crossings](../../../conceptual/python-java-boundary.md)

## Syntax

```python syntax
to_pd_timedelta(dt: Union[None, Duration]) -> pandas.Timedelta
```

## Parameters

<ParamTable>
<Param name="dt" type="Union[None, Duration]">

A Java Duration to convert to a `pandas.Timedelta`. If `None` is provided, `None` is returned.

</Param>
</ParamTable>

## Returns

Returns a `pandas.Timedelta`.

## Examples

```python order=null
from deephaven.time import to_j_duration, to_pd_timedelta

duration = to_j_duration("PT42M")

pd_td = to_pd_timedelta(duration)

print(pd_td, type(pd_td))
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timedelta)
