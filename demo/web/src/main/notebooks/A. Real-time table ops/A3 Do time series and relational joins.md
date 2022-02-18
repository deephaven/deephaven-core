# Time series and relational joins

In our [previous notebook](A2%20Filter%20and%20decorate%20.md), we showed how to filter and decorate our time tables. In this notebook, we show how to perform joins with our time series data.

Let's start again by simulating measurements of our values every minute, but this time using two tables with slightly different timestamps. This is great for a simulation because there's no guarantee that a real example will collect data with exact timestamp matches.

```python
time_interval = expressionToNanos("T1M")
offset_0 = expressionToNanos("10DT2S")
offset_1 = expressionToNanos("10D")
now = currentTime()

daily_data_0 = create_random_table(time_interval, start_time=minus(now, offset_0))
daily_data_1 = create_random_table(time_interval, start_time=minus(now, offset_1))
```

To join these tables together based on the timestamps, we need to use an [as-of join, or `aj`](https://deephaven.io/core/docs/reference/table-operations/join/aj/). As-of joins perform exact matches across all given columns except for the last one, which instead matches based on the closest values.

For an `aj`, the values in the right table are matched to the closest values in the left table without going over the value in the left table. For example, if the right table contains a value `5` and the left table contains values `4` and `6`, the right table's `5` will be matched on the left table's `6`.

Let's join these tables using an `aj` to get a single table with all of our information.

```python
joined_data_aj = daily_data_0.aj(daily_data_1, "Timestamp", "Number1 = Number, Character1 = Character, Boolean1 = Boolean")
```

Deephaven supports another type of as-of-join, a reverse as-of-join. Using `raj`, the values in the right table are matched to the closest values in the left table without going under the left value. For example, if the right table contains a value `5` and the left table contains values `4` and `6`, the right table's `5` will be matched on the left table's `4`.

Let's also join these tables with the `raj` method.

```python
joined_data_raj = daily_data_0.raj(daily_data_1, "Timestamp", "Number1 = Number, Character1 = Character, Boolean1 = Boolean")
```

As-of-joins work very well with time tables that sample at different frequencies. Let's create two new tables, one that samples every second and one that samples every ten seconds, and show what happens when we join them together using `aj` and `raj`.

```python
time_interval_0 = expressionToNanos("T1S")
time_interval_1 = expressionToNanos("T10S")

sample_data_0 = create_random_table(time_interval_0)
sample_data_1 = create_random_table(time_interval_1)

sample_data_aj = sample_data_0.aj(sample_data_1, "Timestamp", "Number1 = Number, Character1 = Character, Boolean1 = Boolean")
sample_data_raj = sample_data_0.raj(sample_data_1, "Timestamp", "Number1 = Number, Character1 = Character, Boolean1 = Boolean")
```
