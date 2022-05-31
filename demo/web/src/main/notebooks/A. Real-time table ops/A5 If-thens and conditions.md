# If-then and conditions

In this notebook, we show how to use if-then statements and conditionals in Deephaven.

Let's start again by simulating some real-time data.

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

time_interval = to_nanos("24:00:00")
offset = to_nanos("24000:00:00")
now_ = now()

daily_data = create_random_table(time_interval, start_time=minus(now_, offset))
```

## Ternaries

Deephaven supports [Java ternaries](https://deephaven.io/core/docs/reference/query-language/control-flow/ternary-if/). We can use these to perform if-else statements on our tables to assign values to a new column based on the if-else evaluation.

```python
daily_data = daily_data.update(formulas=["IsEven = Number%2 == 0 ? true : false"])
```

This query creates a new column `IsEven` in the `daily_data` table.

### Custom methods for ternary conditions

You may want to use a custom method for your condition for your ternary. To do this, you simply use the method instead of the hardcoded conditional. There is a catch - you need to cast the method to `(Boolean)` for Deephaven to recognize it properly.

Let's use the `is_weekday` method in a ternary statement to create a new column that tells us if a given day falls during the week or weekend.

```python
def is_weekday(day_of_week):
    return day_of_week <= 5

daily_data = daily_data.update(formulas=["DayOfWeekInt = dayOfWeek(Timestamp, TZ_NY)"])\
    .update(formulas=["IsWeekday = (Boolean)is_weekday(DayOfWeekInt) ? true: false"])
```
