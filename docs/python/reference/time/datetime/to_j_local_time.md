---
title: to_j_local_time
---

`to_j_local_time` converts a date-time value into a Java `LocalTime`, which is the time that would be read from a clock and does not have a date or time zone.

> [!IMPORTANT]
> In query strings, users should choose the built-in function [`toLocalTime`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#toLocalTime(java.time.ZonedDateTime)) instead of `to_j_local_time`. For more information, see:
>
> - [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
> - [Java/Python boundary crossings](../../../conceptual/python-java-boundary.md)
> - [Auto-imported functions](../../query-language/query-library/auto-imported-functions.md)

## Syntax

```python syntax
to_j_local_time(dt: Union[None, LocalTime, int, str, datetime.time.datetime.datetime, numpy.datetime64, pandas.Timestamp]) -> LocalTime
```

## Parameters

<ParamTable>
<Param name="dt" type="Union[None, LocalTime, int, str, datetime.time, datetime.datetime, numpy.datetime64, pandas.Timestamp]">

A Python local time or local time string.

Time strings can be formatted as `hh:mm:ss[.nnnnnnnnn]`.

</Param>
</ParamTable>

## Returns

Returns a `LocalTime`.

## Examples

```python
from deephaven.time import to_j_local_time

local_time = to_j_local_time("11:11:11.23142")
print(local_time)
```

```python order=null
from deephaven.time import to_j_local_time
import datetime
import numpy as np
import pandas as pd

dt_time = datetime.time(11, 11, 11)
dt_dt = datetime.datetime(2024, 11, 11, 11, 11)
np_dt64 = np.datetime64("2024-04-11T08:00")
pd_timestamp = pd.Timestamp("2024-05-12T11:11")

lt_from_int = to_j_local_time(9000000000000)
lt_from_dt_time = to_j_local_time(dt_time)
lt_from_dt_dt = to_j_local_time(dt_dt)
lt_from_np_dt64 = to_j_local_time(np_dt64)
lt_from_pd_timestamp = to_j_local_time(pd_timestamp)
lt_from_string = to_j_local_time("11:33:22")

print(lt_from_int)
print(lt_from_dt_time)
print(lt_from_dt_dt)
print(lt_from_np_dt64)
print(lt_from_pd_timestamp)
print(lt_from_string)
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_time)
