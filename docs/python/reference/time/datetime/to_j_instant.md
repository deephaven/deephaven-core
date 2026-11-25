---
title: to_j_instant
---

`to_j_instant` converts a date-time value to Java `Instant`.

> [!IMPORTANT]
> In query strings, users should choose the built-in function [`parseInstant`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseInstant(java.lang.String)) instead of `to_j_instant`. For more information, see:
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

Instant strings should be formatted according to the ISO-8601 standard, such as `yyyy-MM-ddTHH:mm:ss[.SSSSSSSSS] TZ`, where `TZ` is a time zone, such as `ET` or `UTC`; for example, `2022-01-01T00:00:00 ET`.

A naive `datetime.datetime`, which has no time zone, is read in the Python process's local time zone, while a naive `pandas.Timestamp` is read as UTC.

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

dt = datetime(2024, 11, 2)
dt_64 = np.datetime64(1, "Y")
dt_pd_timestamp = pd.Timestamp("2024-03-01")

dt_int_inst = to_j_instant(5000000000)
dt_datetime_inst = to_j_instant(dt)
dt_64_inst = to_j_instant(dt_64)
dt_pd_timestamp_inst = to_j_instant(dt_pd_timestamp)
datetime_from_str = to_j_instant("2022-01-01T00:00:00 ET")

print(dt_int_inst)
print(dt_datetime_inst)
print(dt_64_inst)
print(dt_pd_timestamp_inst)
print(datetime_from_str)
```

## Related documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [The Python-Java boundary and how it affects query efficiency](../../../conceptual/python-java-boundary.md)
- [Auto-imported functions](../../query-language/query-library/auto-imported/index.md)
- [`parseInstant` Javadoc](/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseInstant(java.lang.String))
- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_instant)
