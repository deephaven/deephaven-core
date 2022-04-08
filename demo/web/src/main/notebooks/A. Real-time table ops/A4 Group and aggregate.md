# Grouping and aggregating time series data

In this notebook, we show how to perform groupings and aggregations with our time series data.

Let's start again by simulating some real-time data. Here, we'll use a shorter time interval.

```python
from deephaven.time import now, to_nanos, minus

time_interval = to_nanos("00:00:01")
offset = to_nanos("00:01:00")
now_ = now()

daily_data = create_random_table(time_interval, start_time=minus(now_, offset))
```

Let's group this data a few different ways based on the columns we have.

```python
group_by_character = daily_data.group_by(by=["Character"])
group_by_boolean = daily_data.group_by(by=["Boolean"])
group_by_odd_even = daily_data.update(formulas=["IsEven = (Number % 2 == 0)"]).group_by(by=["IsEven"]).drop_columns(["IsEven"])
```

Just like all other real-time operations in Deephaven, these groupings update in real time as the source table updates.

Aggregations allow you to perform operations on grouped data. The query below computes statistics on our data.

```python
from deephaven import agg

average_agg = [
    agg.avg(["Number"])
]

total_average = daily_data.agg_by(average_agg, [])
average_by_character = daily_data.agg_by(average_agg, ["Character"])
average_by_boolean = daily_data.agg_by(average_agg, ["Boolean"])
average_by_even_odd = daily_data.update(formulas=["IsEven = (Number % 2 == 0)"]).agg_by(average_agg, ["IsEven"])
```

We can also compute counts on the data.

```python
boolean_count_agg = [
    agg.count_("BooleanCount")
]
boolean_counts = daily_data.agg_by(boolean_count_agg, ["Boolean"])

character_count_agg = [
    agg.count_("CharacterCount")
]
character_counts = daily_data.agg_by(character_count_agg, ["Character"])

combo_count_avg_agg = [
    agg.count_("ComboCount"),
    agg.avg(["AvgNumber = Number"])
]
combo_counts_avg = daily_data.agg_by(combo_count_avg_agg, ["Boolean", "Character"])
```

Just like the grouped data, these aggregations update in real time. You don't need to recompute any of the averages or counts, Deephaven natively supports these real-time updates.