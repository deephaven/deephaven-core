# Grouping and aggregating time series data

In this notebook, we show how to perform groupings and aggregations with our time series data.

Let's start again by simulating some real-time data. Here, we'll use a shorter time interval.

```python
from deephaven.DateTimeUtils import currentTime, expressionToNanos, minus

time_interval = expressionToNanos("T1S")
offset = expressionToNanos("T60S")
now = currentTime()

daily_data = create_random_table(time_interval, start_time=minus(now, offset))
```

Let's group this data a few different ways based on the columns we have.

```python
group_by_character = daily_data.groupBy("Character")
group_by_boolean = daily_data.groupBy("Boolean")
group_by_odd_even = daily_data.update("IsEven = (Number % 2 == 0)").groupBy("IsEven").dropColumns("IsEven")
```

Just like all other real-time operations in Deephaven, these groupings update in real time as the source table updates.

Aggregations allow you to perform operations on grouped data. The query below computes statistics on our data.

```python
from deephaven import Aggregation as agg, as_list

average_agg = as_list([
    agg.AggAvg("Number")
])

total_average = daily_data.aggBy(average_agg)
average_by_character = daily_data.aggBy(average_agg, "Character")
average_by_boolean = daily_data.aggBy(average_agg, "Boolean")
average_by_even_odd = daily_data.update("IsEven = (Number % 2 == 0)").aggBy(average_agg, "IsEven")
```

We can also compute counts on the data.

```python
boolean_count_agg = as_list([
    agg.AggCount("BooleanCount")
])
boolean_counts = daily_data.aggBy(boolean_count_agg, "Boolean")

character_count_agg = as_list([
    agg.AggCount("CharacterCount")
])
character_counts = daily_data.aggBy(character_count_agg, "Character")

combo_count_avg_agg = as_list([
    agg.AggCount("ComboCount"),
    agg.AggAvg("AvgNumber = Number")
])
combo_counts_avg = daily_data.aggBy(combo_count_avg_agg, "Boolean", "Character")
```

Just like the grouped data, these aggregations update in real time. You don't need to recompute any of the averages or counts, Deephaven natively supports these real-time updates.
