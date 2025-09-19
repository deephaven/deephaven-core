---
title: to_date
---

`to_date` converts a Java date-time to a `datetime.date`.

> [!WARNING]
> `to_date` should never be used in query strings.

## Syntax

```python syntax
to_date(dt: Union[None, LocalDate, ZonedDateTime]) -> datetime.date
```

## Parameters

<ParamTable>
<Param name="dt" type="Union[None, LocalDate, ZonedDateTime]">

A Java date-time to convert to a `datetime.date`. If `None` is provided, `None` is returned.

</Param>
</ParamTable>

## Returns

Returns a `datetime.date`.

## Examples

```python order=null
from deephaven.time import dh_today, to_j_local_date, to_date, to_j_zdt

# create a LocalDate
date_str = dh_today()
local_date = to_j_local_date(date_str)

# create a ZonedDateTime
date_str_tz = date_str + " ET"
zdt = to_j_zdt(date_str_tz)

# convert LocalDate and ZonedDateTime to `datetime.date`
date_local = to_date(local_date)
date_zoned = to_date(zdt)

print(date_local)
print(date_zoned)
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.to_date)
