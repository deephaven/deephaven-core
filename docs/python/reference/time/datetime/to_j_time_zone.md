---
title: to_j_time_zone
---

`to_j_time_zone` converts a time zone value into a Java `TimeZone`.

> [!IMPORTANT]
> In query strings, users should choose the built-in function [`parseTimeZone`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseTimeZone(java.lang.String)) instead of `to_j_time_zone`. For more information, see:
>
> - [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
> - [Java/Python boundary crossings](../../../conceptual/python-java-boundary.md)
> - [Auto-imported functions](../../query-language/query-library/auto-imported-functions.md)

## Syntax

```
to_j_time_zone(tz: Union[None, TimeZone, str, datetime.tzinfo, datetime.datetime, pandas.Timestamp]) -> TimeZone
```

## Parameters

<ParamTable>
<Param name="tz" type="Union[None, TimeZone, str, datetime.tzinfo, datetime.datetime, pandas.Timestamp]">

The time zone value to convert to a Java `TimeZone`.

</Param>
</ParamTable>

## Returns

Returns a `TimeZone`.

## Examples

```python order=null
from deephaven.time import to_j_time_zone
import datetime
import pandas as pd

dt_tz = datetime.timezone(datetime.timedelta(0, 0, 0, 0, 0, 3), "Tzone")
pd_timestamp = pd.Timestamp(1513393355, unit="s", tz="US/Pacific")
tz_str = "UTC"

tz_from_dt_tz = to_j_time_zone(dt_tz)
tz_from_pd_timestamp = to_j_time_zone(pd_timestamp)
tz_from_str = to_j_time_zone(tz_str)

print(tz_from_dt_tz)
print(tz_from_pd_timestamp)
print(tz_from_str)
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_time_zone)
