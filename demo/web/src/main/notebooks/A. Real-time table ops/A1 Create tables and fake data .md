# Create tables and fake data

Throughout this demo notebook series, we show many of the ways to interact with real-time data in Deephaven. Here, we create some tables with fake data; in other notebooks, we show how to perform table operations on that data. Knowing how to create fake ticking tables is useful for familiarizing yourself with Deephaven, but also for working on proof of concepts without necessarily having a complete dataset.

The following Python code contains a method that creates a table of random integers and characters, with each row in the table also containing a timestamp.

```python
from deephaven import DynamicTableWriter
from deephaven.DateTimeUtils import plus
import deephaven.Types as dht

import random
import string
import threading

def create_random_table(number_of_rows, start_time, time_offset):
    """
    Creates a Deephaven table containing rows of random integers from 1 to 99, random
    uppercase characters, and timestamps.
    
    Parameters:
        number_of_rows (int): The number of rows that the resulting table will contain.
        start_time (DateTime): The Deephaven date-time of the first row in the table.
        time_offset (Period): A Period object representing the timestamp difference between
            each row in the table.
    Returns:
        A Deephaven Table containing the random data.
    """
    def thread_function(number_of_rows, start_time, time_offset, table_writer):
        time = start_time
        for i in range(number_of_rows):
            random_number = random.randint(1, 100)
            random_character = random.choice(string.ascii_uppercase)
            random_boolean = random.choice([True, False])
            table_writer.logRow(time, random_number, random_character, random_boolean)
            time = plus(time, time_offset)

    column_names = ["DateTime", "Number", "Character", "Boolean"]
    column_types = [dht.datetime, dht.int_, dht.string, dht.bool_]
    table_writer = DynamicTableWriter(column_names, column_types)

    thread = threading.Thread(target=thread_function, args=(number_of_rows, start_time, time_offset, table_writer))
    thread.start()

    return table_writer.getTable()
```

We can use this method to create some tables with random data.

```python
from deephaven.DateTimeUtils import Period, convertDateTime

start_time = convertDateTime("2000-01-01T00:00:00 NY")

time_offset = Period("T1S")
random_table_1_second_offset_small = create_random_table(1000, start_time, time_offset)
random_table_1_second_offset_large = create_random_table(100000, start_time, time_offset)

time_offset = Period("T10S")
random_table_10_seconds_offset_small = create_random_table(1000, start_time, time_offset)
random_table_10_seconds_offset_large = create_random_table(100000, start_time, time_offset)

time_offset = Period("T0.1S")
random_table_tenth_second_offset_small = create_random_table(1000, start_time, time_offset)
random_table_tenth_second_offset_large = create_random_table(100000, start_time, time_offset)
```

[The next notebook](A2%20Filter%20and%20decorate.md) will show how to filter and decorate this data.
