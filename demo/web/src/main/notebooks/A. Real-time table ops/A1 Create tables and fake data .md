# Create tables and fake data

Throughout this demo notebook series, we show many of the ways to interact with real-time data in Deephaven. Here, we create some tables with fake data; in other notebooks, we show how to perform table operations on that data. Knowing how to create fake ticking tables can be useful for familiarizing yourself with Deephaven and to work on proof of concepts without necessarily having a complete dataset.

The following Python code contains a method that creates a table with random integers and characters in it, with each row in the table containing a time-stamp as well.

```python
from deephaven import DynamicTableWriter
from deephaven.DateTimeUtils import plus
import deephaven.Types as dht

import random
import string

def create_random_table(number_of_rows, start_time, time_offset):
    column_names = ["DateTime", "Number", "Character"]
    column_types = [dht.datetime, dht.int_, dht.string]
    table_writer = DynamicTableWriter(column_names, column_types)

    time = start_time
    for i in range(number_of_rows):
        random_number = random.randint(1, 100)
        random_character = random.choice(string.ascii_uppercase)
        table_writer.logRow(time, random_number, random_character)
        time = plus(time, time_offset)

    return table_writer.getTable()
```

We can use this method to create some tables with random data in it.

```python
from deephaven.DateTimeUtils import Period, convertDateTime

start_time = convertDateTime("2000-01-01T00:00:00 NY")

t1 = create_random_table(5, start_time, time_offset)
t2 = create_random_table(50, start_time, time_offset)

time_offset = Period("T10S")
t3 = create_random_table(5, start_time, time_offset)
t4 = create_random_table(50, start_time, time_offset)

time_offset = Period("T0.1S")
t5 = create_random_table(5, start_time, time_offset)
t6 = create_random_table(50, start_time, time_offset)
```

[The next notebook](A2%20Filter%20and%20decorate.md) will show how to filter and decorate this data.
