# Time methods

In this notebook, we show how to use Deephaven's time methods on time series data.

Let's start again by simulating some real-time data, but this time with a shorter time interval.

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

time_interval = to_nanos("00:00:01")
offset = to_nanos("00:16:40")
now_ = now()

daily_data = create_random_table(time_interval, start_time=minus(now_, offset))
```

## Datetime arithmethic

Deephaven supports DateTime arithmetic through methods such as [plus](https://deephaven.io/core/docs/reference/time/datetime/plus/) and [minus](https://deephaven.io/core/docs/reference/time/datetime/minus/).

This example shows how to subtract 2 hours from a timestamp:

```python
from deephaven.time import to_period

two_hours = to_period("T2H")

daily_data = daily_data.update(formulas=["TimestampTwoHoursBefore = minus(Timestamp, two_hours)"])
```

## Downsampling

Downsampling can be done in Deephaven by deciding how to group your data, and then using an appropriate aggregation on the grouped data.

With time series data, binning methods like [lowerBin](https://deephaven.io/core/docs/reference/time/datetime/lowerBin/) can be used to group by timestamps.

This example shows how to group timestamps by the minute, and then store the sum of the `Number` column for each minute.

```python
from deephaven.time import to_nanos
from deephaven import agg

agg_list = [
    agg.sum_(["Number"])
]

nanos_bin = to_nanos("00:01:00")

daily_data_binned = daily_data.update(formulas=["TimestampMinute = lowerBin(Timestamp, nanos_bin)"])\
    .drop_columns(["Timestamp"])\
    .agg_by(agg_list, ["TimestampMinute"])
```

## as-of joins

[as-of joins](https://deephaven.io/core/docs/reference/table-operations/join/aj/) allow you to join data without exact matches. This works very well with time-series data.

This example shows how to join two time-series tables that have different densities in timestamps.

```python
time_interval = to_nanos("00:00:10")
offset = to_nanos("00:16:40")
now = now()

daily_data_two = create_random_table(time_interval, start_time=minus(now, offset))

joined_daily_data = daily_data.aj(table=daily_data_two, on=["Timestamp"], joins=["NumberTwo = Number", "CharacterTwo = Character", "BooleanTwo = Boolean"])
```
