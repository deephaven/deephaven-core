---
title: to_j_period
---

`to_j_period` converts a date-time value into a Java `Period`, which is a length of time expressed as `PnYnMnWnD`, with `n` being numeric values for years, months, weeks, and days.

> [!IMPORTANT]
> In query strings, users should choose the built-in function [`parsePeriod`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parsePeriod(java.lang.String)) instead of `to_j_period`. For more information, see:
>
> - [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
> - [Java/Python boundary crossings](../../../conceptual/python-java-boundary.md)
> - [Auto-imported functions](../../query-language/query-library/auto-imported-functions.md)

## Syntax

```
to_j_period(dt: Union[None, Period, str, datetime.timedelta, numpy.timedelta64, pandas.Timedelta]) -> Period
```

## Parameters

<ParamTable>
<Param name="s" type="str">

A Python period or period string.

Period strings should be in the form of `PnYnMnWnD`, with `n` being numeric values; e.g., P1W for one week, P1M for one month, or P1W5D for one week plus five days.

</Param>
</ParamTable>

## Returns

Returns a Java `Period`.

## Examples

```python order=null
from deephaven.time import to_j_period
import datetime
import numpy as np
import pandas as pd

p1 = datetime.timedelta(21)
p2 = np.timedelta64(1, "D")
p3 = pd.Timedelta(11, "d")

period_from_timedelta = to_j_period(p1)
period_from_timedelta64 = to_j_period(p2)
period_from_pd_timedelta = to_j_period(p3)
period_from_string = to_j_period("P1W5D")

print(period_from_timedelta)
print(period_from_timedelta64)
print(period_from_pd_timedelta)
print(period_from_string)
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_period)
