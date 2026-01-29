---
title: to_j_zdt
---

`to_j_zdt` converts a date-time value to a Java `ZonedDateTime`.

> [!IMPORTANT]
> In query strings, users should choose the built-in function [`parseZonedDateTime`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseZonedDateTime(java.lang.String)) instead of `to_j_zdt`. For more information, see:
>
> - [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
> - [Java/Python boundary crossings](../../../conceptual/python-java-boundary.md)
> - [Auto-imported functions](../../query-language/query-library/auto-imported-functions.md)

## Syntax

```
to_j_zdt(dt: Union[None, ZonedDateTime, str, datetime.datetime, numpy.datetime64, pandas.Timestamp]) -> ZonedDateTime
```

## Parameters

<ParamTable>
<Param name="dt" type="Union[None, ZonedDateTime, str, datetime.datetime, numpy.datetime64, pandas.Timestamp]">

A date-time value to convert to a Java `ZonedDateTime`.

ZonedDateTime strings should be formatted according to the ISO-8601 standard, which is `yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ`, where `TZ` is the time zone offset from UTC; e.g., `2022-01-01T00:00:00 ET`.

</Param>
</ParamTable>

## Returns

Returns a `ZonedDateTime` representation of the date-time string.

## Examples

```python order=:log
from deephaven.time import to_j_zdt
import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo
import datetime

date_string = "2024-08-01T12:00:00 UTC"

z_datetime = datetime.datetime.now(ZoneInfo("America/Chicago"))

z_timestamp = pd.Timestamp("2024-08-01 12:00:00", tz="America/New_York")

zdt_from_dt = to_j_zdt(z_datetime)
zdt_from_timestamp = to_j_zdt(z_timestamp)
zdt_from_str = to_j_zdt(date_string)

print(zdt_from_dt)
print(zdt_from_timestamp)
print(zdt_from_str)
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_zdt)
