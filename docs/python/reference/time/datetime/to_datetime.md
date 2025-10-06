---
title: to_datetime
---

`to_datetime` converts a Java date-time to a `datetime.datetime`.

> [!WARNING]
> `to_datetime` should never be used in query strings.

## Syntax

```python syntax
to_datetime(dt: Union[None, Instant, ZonedDateTime]) -> datetime.datetime
```

## Parameters

<ParamTable>
<Param name="dt" type="Union[None, Instant, ZonedDateTime]">

A Java date-time to convert to a `datetime.datetime`. If `None` is provided, `None` is returned.

</Param>
</ParamTable>

## Returns

Returns a `datetime.datetime`.

## Examples

```python order=:log
from deephaven.time import dh_now, to_datetime, to_j_zdt

java_date = dh_now()

zdt = to_j_zdt("2024-03-22T11:11:11.23142 UTC")

datetime = to_datetime(java_date)

datetime_from_zdt = to_datetime(zdt)

print(datetime)
print(datetime_from_zdt)
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.to_datetime)
