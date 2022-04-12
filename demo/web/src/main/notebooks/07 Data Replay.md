# Data Replay

Deephaven excels at handling live data. Integrating historical data into real-time analysis is common in a multitude of fields, including machine learning, validation, modeling, simulation, and forecasting.

Here, we show how to take historical data and play it back as real-time data based on timestamps in a table. This is useful for demonstration purposes, but this example could be easily extended towards a variety of real-world applications.

To start, let's make a sample table containing random numbers generated at certain historical timestamps.

```python
from deephaven import DynamicTableWriter
from deephaven.time import to_period, to_datetime, plus_period
from deephaven.dtypes import DateTime, int_

import random

table_writer = DynamicTableWriter(
    {
        "DateTime": DateTime,
        "Number": int_,
    }
)

time = to_datetime("2000-01-01T00:00:00 NY")
time_offset = to_period("T1S")

result = table_writer.table

for i in range(100):
    random_number = random.randint(1, 100)

    table_writer.write_row(time, random_number)
    time = plus_period(time, time_offset)
```

After running this code, we can see that the `result` table contains 100 entries of random numbers with each number having a historical timestamp.

So how do we replay this data? Using the [`replayer`](https://deephaven.io/core/docs/reference/table-operations/create/Replayer/) object, we can specify a start and end time, and apply this to our table.

```python
from deephaven.replay import TableReplayer

start_time = to_datetime("2000-01-01T00:00:00 NY")
end_time = to_datetime("2000-01-01T00:01:40 NY")

replayer = TableReplayer(start_time, end_time)
replayed_table = replayer.add_table(result, "DateTime")

replayer.start()
```

After running this code, the `replayed_result` table begins updating in "real-time" with our historical data. Since each of our timestamps are one second apart, the table updates with a new row every second. This gives us an exact replication of how our initial table would have been populated in real-time.

Deephaven table operations do not discriminate between dynamic or static data; we can apply the same table operations to this table as we would any table.

```python
from deephaven.replay import TableReplayer
from deephaven import agg

start_time = to_datetime("2000-01-01T00:00:00 NY")
end_time = to_datetime("2000-01-01T00:01:40 NY")

replayer = TableReplayer(start_time, end_time)
replayed_table = replayer.add_table(result, "DateTime")

replayer.start()

agg_list = [
    agg.avg(["Number"])
]

replayed_average = replayed_table.agg_by(agg_list, [])
```

With this example, we can re-run our replay and see our average value updating in real-time.