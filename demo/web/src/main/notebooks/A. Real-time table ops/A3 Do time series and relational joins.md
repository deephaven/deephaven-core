# Time series and relational joins

In this notebook, we show how to perform joins with our time series data.

Let's start again by simulating measurements of our values every minute, but this time using two tables with slightly different timestamps. This is great for a simulation because there's no guarantee that a real example will collect data with exact timestamp matches.

```python
from deephaven import time_table

import random
import string

def random_int():
    return random.randint(1,100)
def random_character():
    return random.choice(string.ascii_uppercase)
def random_boolean():
    return random.choice([True, False])

def create_random_table(time_interval, start_time=None):
    """
    Creates a Deephaven table containing rows of random integers from 1 to 99, random
    uppercase characters, and timestamps.

    Parameters:
        time_interval (str||int): String or int representation of the time interval between rows.
        start_time (str||DateTime): Optional string or DateTime representation of the start time.
    Returns:
        A Deephaven Table containing the random data.
    """
    table = None
    if start_time is None:
        table = time_table(time_interval)
    else:
        table = time_table(period=time_interval, start_time=start_time)

    return table.update(formulas=["Number = (int)(byte)random_int()", "Character = (String)random_character()", "Boolean = (boolean)random_boolean()"])

from deephaven.time import now, to_nanos, minus

time_interval = to_nanos("00:01:00")
offset_0 = to_nanos("240:00:02")
offset_1 = to_nanos("240:00:00")
now_ = now()

daily_data_0 = create_random_table(time_interval, start_time=minus(now_, offset_0))
daily_data_1 = create_random_table(time_interval, start_time=minus(now_, offset_1))
```

To join these tables together based on the timestamps, we need to use an [as-of join, or `aj`](https://deephaven.io/core/docs/reference/table-operations/join/aj/). As-of joins perform exact matches across all given columns except for the last one, which instead matches based on the closest values.

For an `aj`, the values in the right table are matched to the closest values in the left table without going over the value in the left table. For example, if the right table contains a value `5` and the left table contains values `4` and `6`, the right table's `5` will be matched on the left table's `6`.

Let's join these tables using an `aj` to get a single table with all of our information.

```python
joined_data_aj = daily_data_0.aj(table=daily_data_1, on=["Timestamp"], joins=["Number1 = Number", "Character1 = Character", "Boolean1 = Boolean"])
```

Deephaven supports another type of as-of join, a `raj`. For a `raj`, the values in the right table are matched to the closest values in the left table without going under the left value. For example, if the right table contains a value `5` and the left table contains values `4` and `6`, the right table's `5` will be matched on the left table's `4`.

Let's also join these tables using a `raj`.

```python
joined_data_raj = daily_data_0.raj(table=daily_data_1, on=["Timestamp"], joins=["Number1 = Number", "Character1 = Character", "Boolean1 = Boolean"])
```

As of joins work very well with time-tables that sample at different frequencies. Let's create two new tables, one that samples every second and one that samples every ten seconds, and show what happesn when we join them together using `aj` and `raj`.

```python
time_interval_0 = to_nanos("00:00:01")
time_interval_1 = to_nanos("00:00:10")

sample_data_0 = create_random_table(time_interval_0)
sample_data_1 = create_random_table(time_interval_1)

sample_data_aj = sample_data_0.aj(table=sample_data_1, on=["Timestamp"], joins=["Number1 = Number", "Character1 = Character", "Boolean1 = Boolean"])
sample_data_raj = sample_data_0.raj(table=sample_data_1, on=["Timestamp"], joins=["Number1 = Number", "Character1 = Character", "Boolean1 = Boolean"])
```
