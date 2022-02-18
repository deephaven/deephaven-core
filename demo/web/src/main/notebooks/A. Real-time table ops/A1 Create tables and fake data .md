# Create tables and fake data

Throughout this demo notebook series, we show many of the ways to interact with real-time data in Deephaven. Here, we create some tables with fake data; in other notebooks, we show how to perform table operations on that data. Knowing how to create fake ticking tables is useful for familiarizing yourself with Deephaven, but also for working on proof of concepts without necessarily having a complete dataset.

`timeTable` is a great tool to simulate real-time data. We can use this and Python's `random` library to generate some fake data.

```python
from deephaven.TableTools import timeTable

import random
import string

def random_int():
    return random.randint(1,100)
def random_character():
    return random.choice(string.ascii_uppercase)
def random_boolean():
    return random.choice([True, False])

table = timeTable("00:00:01").update("Number = (int)random_int()")\
    .update("Character = (String)random_character()")\
    .update("Boolean = (boolean)random_boolean()")
```

Let's wrap `timeTable` with a method and parameterize the time intervals and start times. This will allow us to reuse the table throughout the notebooks.

```python
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
        table = timeTable(time_interval)
    else:
        table = timeTable(start_time, time_interval)

    return table.update("Number = (int)random_int()")\
            .update("Character = (String)random_character()")\
            .update("Boolean = (boolean)random_boolean()")
```

We can use this method to create some tables with random data.

```python
random_table_1_second_offset = create_random_table("00:00:01")
random_table_10_seconds_offset = create_random_table("00:00:10")
random_table_tenth_second_offset = create_random_table("00:00:00.1")
```

[The next notebook](A2%20Filter%20and%20decorate.md) will show how to filter and decorate this data.
