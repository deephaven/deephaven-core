---
title: to_j_instant
---

`to_j_instant` converts a date-time value to Java `Instant`.

> [!IMPORTANT]
> In query strings, users should choose the built-in function [`toInstant`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseInstant(java.lang.String)) instead of `to_j_instant`. For more information, see:
>
> - [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
> - [Java/Python boundary crossings](../../../conceptual/python-java-boundary.md)
> - [Auto-imported functions](../../query-language/query-library/auto-imported/index.md)

## Syntax

```python syntax
to_j_instant(
  dt: Union[None, Instant, int, str, datetime.datetime, numpy.datetime64, pandas.Timestamp]
  ) -> Instant
```

## Parameters

<ParamTable>
<Param name="dt" type="Union[None, Instant, int, str, datetime.datetime, numpy.datetime64, pandas.Timestamp]">

A date-time value to convert to a Java Instant.

Instant strings should be formatted according to the ISO-8601 standard, which is `yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ`, where `TZ` is the time zone offset from UTC; e.g., `2022-01-01T00:00:00 ET`.

</Param>
</ParamTable>

## Returns

Returns an `Instant`.

## Examples

```python order=null
from deephaven.time import to_j_instant
from datetime import datetime
import numpy as np
import pandas as pd

datetime = datetime(2024, 11, 2)
dt_64 = np.datetime64(1, "Y")
dt_pd_timestamp = pd.Timestamp("2024-03-01")

dt_int_inst = to_j_instant(5000000000)
dt_datetime_inst = to_j_instant(datetime)
dt_64_inst = to_j_instant(dt_64)
dt_pd_timestamp_inst = to_j_instant(dt_pd_timestamp)
datetime_from_str = to_j_instant("2022-01-01T00:00:00 ET")

print(dt_int_inst)
print(dt_datetime_inst)
print(dt_64_inst)
print(dt_pd_timestamp_inst)
print(datetime_from_str)
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_instant)
