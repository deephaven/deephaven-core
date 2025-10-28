---
title: How do I extract a list of distinct dates from a table?
sidebar_label: How do I extract a list of distinct dates from a table?
---

_I have a table with a date column that contains many repeated dates. How can I get a list of the unique dates?_

Use [`select_distinct`](../table-operations/filter/select-distinct.md) to get the unique dates, then convert the result to a Python list, NumPy array, or Pandas DataFrame:

```python test-set=dates order=source,distinct_dates
from deephaven import empty_table
from deephaven.time import to_j_instant

# Sample table with many rows and repeated dates across multiple timestamps
start_date = to_j_instant("2024-01-15T09:00:00 ET")

source = empty_table(50).update(
    [
        "Timestamp = start_date + (long)(i * HOUR)",
        "Date = toLocalDate(Timestamp, 'ET')",
        "Value = randomDouble(100.0, 500.0)",
    ]
)

# Get distinct dates - reduces 50 rows to just the unique dates
distinct_dates = source.select_distinct("Date")
```

## Convert to NumPy array

Use [`deephaven.numpy.to_numpy`](../../how-to-guides/use-numpy.md) to convert to a NumPy array:

```python test-set=dates
from deephaven import numpy as dhnp
import numpy as np

# Convert to NumPy array
date_array = dhnp.to_numpy(distinct_dates)
print(f"Date array shape: {date_array.shape}")
print(f"First date: {date_array[0][0]}")
```

## Convert to Pandas DataFrame

Use [`deephaven.pandas.to_pandas`](../../how-to-guides/use-pandas.md) to convert to a Pandas DataFrame:

```python test-set=dates
from deephaven import pandas as dhpd
import pandas as pd

# Convert to Pandas DataFrame
date_df = dhpd.to_pandas(distinct_dates)
print(f"DataFrame:\n{date_df}")

# Access as a Python list from the DataFrame
date_list = date_df["Date"].tolist()
print(f"Date list length: {len(date_list)}")
```

This approach works for any column type, not just dates. Use `select_distinct` to get unique values, then choose the conversion method that best fits your use case.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
