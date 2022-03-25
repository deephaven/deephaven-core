# Time methods

In this notebook, we show how to use Deephaven's time methods on time series data.

Let's start again by simulating some real-time data, but this time with a shorter time interval.

```python
from deephaven.DateTimeUtils import currentTime, expressionToNanos, minus

time_interval = expressionToNanos("T1S")
offset = expressionToNanos("T1000S")
now = currentTime()

daily_data = create_random_table(time_interval, start_time=minus(now, offset))
```

## Datetime arithmethic

Deephaven supports DateTime arithmetic through methods such as [plus](https://deephaven.io/core/docs/reference/time/datetime/plus/) and [minus](https://deephaven.io/core/docs/reference/time/datetime/minus/).

This example shows how to subtract 2 hours from a timestamp:

```python
from deephaven.DateTimeUtils import convertPeriod

two_hours = convertPeriod("T2H")

daily_data = daily_data.update("TimestampTwoHoursBefore = minus(Timestamp, two_hours)")
```

## Downsampling

Downsampling can be done in Deephaven by deciding how to group your data, and then using an appropriate aggregation on the grouped data.

With time series data, binning methods like [lowerBin](https://deephaven.io/core/docs/reference/time/datetime/lowerBin/) can be used to group by timestamps.

This example shows how to group timestamps by the minute, and then store the sum of the `Number` column for each minute.

```python
from deephaven.DateTimeUtils import expressionToNanos
from deephaven import Aggregation as agg, as_list

agg_list = as_list([
    agg.AggSum("Number")
])

nanos_bin = expressionToNanos("T1M")

daily_data_binned = daily_data.update("TimestampMinute = lowerBin(Timestamp, nanos_bin)")\
    .dropColumns("Timestamp")\
    .aggBy(agg_list, "TimestampMinute")
```

## as-of joins

[as-of joins](https://deephaven.io/core/docs/reference/table-operations/join/aj/) allow you to join data without exact matches. This works very well with time-series data.

This example shows how to join two time-series tables that have different densities in timestamps.

```python
time_interval = expressionToNanos("T10S")
offset = expressionToNanos("T1000S")
now = currentTime()

daily_data_two = create_random_table(time_interval, start_time=minus(now, offset))

joined_daily_data = daily_data.aj(daily_data_two, "Timestamp", "NumberTwo = Number, CharacterTwo = Character, BooleanTwo = Boolean")
```
