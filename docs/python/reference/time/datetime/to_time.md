---
title: to_time
---

`to_time` converts a Java date-time to a `datetime.time`.

> [!WARNING]
> Avoid [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) functions in query strings. Because [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) provides functions that convert between Python and Java types, every call crosses the [Python-Java boundary](../../../conceptual/python-java-boundary.md), which slows performance. For more information, see:
>
> - [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
> - [Java/Python boundary crossings](../../../conceptual/python-java-boundary.md)

## Syntax

```python syntax
to_time(dt: Union[None, LocalTime, ZonedDateTime]) -> datetime.time
```

## Parameters

<ParamTable>
<Param name="dt" type="Union[None, LocalTime, ZonedDateTime]">

A Java date-time to convert to a `datetime.time`. If `None` is provided, `None` is returned.

</Param>
</ParamTable>

## Returns

Returns a `datetime.time`.

## Examples

```python order=null
from deephaven.time import to_j_local_time, to_time, to_j_local_date, to_j_zdt

local_time = to_j_local_time("11:11:11.23142")
zdt = to_j_zdt("2023-03-12T11:34:02 ET")

time1 = to_time(local_time)
time2 = to_time(zdt)

print(time1)
print(time2)
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.to_time)
