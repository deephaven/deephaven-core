---
title: to_j_local_date
---

`to_j_local_date` converts a date-time value into a Java `LocalDate`, which is a date without a time or time zone.

> [!IMPORTANT]
> In query strings, users should choose the built-in function [`toLocalDate`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#toLocalDate(java.time.Instant,java.time.ZoneId)) instead of `to_j_local_date`. For more information, see:
>
> - [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
> - [Java/Python boundary crossings](../../../conceptual/python-java-boundary.md)
> - [Auto-imported functions](../../query-language/query-library/auto-imported/index.md)

## Syntax

```python syntax
to_j_local_date(
  dt: Union[None, LocalDate, str, datetime.date, datetime.time, datetime.datetime, numpy.datetime64, pandas.Timestamp]
  ) -> LocalDate
```

## Parameters

<ParamTable>
<Param name="dt" type="Union[None, LocalDate, str, datetime.date, datetime.time, datetime.datetime, numpy.datetime64, pandas.Timestamp]">

The date-time value to convert to a Java LocalDate.

A string in the form of `[-]PTnHnMnS`, with `n` being numeric values; e.g., `PT1H` for one hour, `PT1M` for one minute, or `-PT1M5.5S` for negative one minute and 5.5 seconds.

</Param>
</ParamTable>

## Returns

Returns a `LocalDate`.

## Examples

```python
from deephaven.time import to_j_local_date, dh_today

ld = to_j_local_date(dh_today())
print(ld)
```

```python order=null
from deephaven.time import to_j_local_date
import numpy as np
import pandas as pd
import datetime

dt_date = datetime.date(2024, 9, 2)
dt_dt = datetime.datetime(2024, 9, 2)
np_dt64 = np.datetime64(12, "h")
pd_dt = pd.Timestamp("2024-11-11")

dt_date_ld = to_j_local_date(dt_date)
dt_dt_ld = to_j_local_date(dt_dt)
np_dt64_ld = to_j_local_date(np_dt64)
pd_dt_ld = to_j_local_date(pd_dt)
date_str_ld = to_j_local_date("2024-10-12")

print(dt_date_ld)
print(dt_dt_ld)
print(np_dt64_ld)
print(pd_dt_ld)
print(date_str_ld)
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_date)
