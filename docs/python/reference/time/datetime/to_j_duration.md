---
title: to_j_duration
---

`to_j_duration` converts a time duration value to a Java `Duration`, which is a unit of time in terms of clock time (counting days, hours, minutes, seconds, and nanoseconds).

> [!IMPORTANT]
> In query strings, users should choose the built-in function [`parseDuration`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseDuration(java.lang.String)) instead of `to_j_duration`. For more information, see:
>
> - [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
> - [Java/Python boundary crossings](../../../conceptual/python-java-boundary.md)
> - [Auto-imported functions](../../query-language/query-library/auto-imported/index.md)

## Syntax

```python syntax
to_duration(dt: Union[None, Duration, int, str, datetime.timedelta, numpy.timedelta64, pandas.Timedelta]) -> Duration
```

## Parameters

<ParamTable>
<Param name="dt" type="Union[None, Duration, int, str, datetime.timedelta, numpy.timedelta64, pandas.Timedelta]">

A date-time value to convert to a Java Duration.

Strings can be formatted according to the ISO-8601 standard, which is `"[-]PTnHnMnS.nnnnnn"`, with `n` being numeric values; e.g., `PT1H` for one hour, `PT1M` for one minute, or `-PT1M5.5S` for negative one minute and 5.5 seconds.

Strings can also be formatted as `"[-]PT00:00:00.000000"`.

</Param>
</ParamTable>

## Returns

Returns a [`Duration`](../../query-language/types/durations.md) representation of the duration string.

## Examples

```python order=null
from deephaven.time import to_j_duration
from datetime import timedelta
import numpy as np
import pandas as pd

delta = timedelta(days=40, seconds=33, microseconds=21)

timedelta_np = np.timedelta64(1, "M")

timedelta_pd = pd.Timedelta(1, "d")

duration_from_string = to_j_duration("PT1H5M-5S")
duration_from_int = to_j_duration(500_000_000_000)
duration_from_timedelta = to_j_duration(delta)
duration_from_np_timedelta = to_j_duration(timedelta_np)
duration_from_pd_timedelta = to_j_duration(timedelta_pd)

print(duration_from_string)
print(duration_from_int)
print(duration_from_timedelta)
print(duration_from_np_timedelta)
print(duration_from_pd_timedelta)
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_duration)
