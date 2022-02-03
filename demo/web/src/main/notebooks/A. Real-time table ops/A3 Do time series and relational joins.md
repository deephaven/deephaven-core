# Time series and relational joins

In our [previous notebook](A2%20Filter%20and%20decorate%20.md), we showed how to filter and decorate our time tables. In this notebook, we show how to perform joins with our time series data.

Let's start again by simulating a year's worth of daily measurements, but this time using two tables with slightly different timestamps. This is great for a simulation because there's no guarantee that a real example will collect data with exact timestamp matches.

```python
time_offset = Period("1D")

start_times = [
    convertDateTime("2020-01-01T00:00:00 NY"),
    convertDateTime("2020-01-01T00:00:02 NY")
]

daily_data_0 = create_random_table(365, start_times[0], time_offset)
daily_data_1 = create_random_table(365, start_times[1], time_offset)
```

To join these tables together based on the timestamps, we need to use an [as-of join, or `aj`](https://deephaven.io/core/docs/reference/table-operations/join/aj/). As-of joins perform exact matches across all given columns except for the last one, which instead matches based on the closest values.

For an `aj`, the values in the right table are matched to the closest values in the left table without going over the value in the left table. For example, if the right table contains a value `5` and the left table contains values `4` and `6`, the right table's `5` will be matched on the left table's `6`.

Let's join these tables using an `aj` to get a single table with all of our information.

```python
joined_data_aj = daily_data_0.aj(daily_data_1, "DateTime", "Number1 = Number, Character1 = Character, Boolean1 = Boolean")
```

If you look at the `joined_data_aj` table, you may not see what you'd expect. Specifically, the first row won't have any values from our `daily_data_1` table, and the last row of the `daily_data_1` table isn't present. What happened?

Remember that an `aj` works based on a search where values in the right table are matched to the closest values in the left table without going over the left value: 

- If there's a value in the left table that doesn't match a value in the right table, the created row will have `NULL` values for what would have been the values from the right table. 
- If there's a value in the right table that doesn't match a value in the left table, the row in the right table won't be included in the joined table.

When looking at the first row in our tables, the timestamp for the right table is `2020-01-01T00:00:02.000` and the timestamp for the left table is `2020-01-01T00:00:00.000`. Since these are the lowest values in our table, there's no match for the left table's timestamp since all of the values in the right table are greater than it, resulting in the row with `NULL` values.

This also explains how the last row in `daily_data_1` is lost. The timestamp value of the last row in `daily_data_1` is `2020-12-29T00:00:02.000`. Since all the values in the left table are less than this value, there can't be a match without going over the left value, resulting in this row being lost.

How can these tables join as expected? We could flip the left and right tables, but then our timestamp column will contain the values in `daily_data_1`, which are a bit messy. Instead, we can use a [reverse as-of join, or `raj`](https://deephaven.io/core/docs/reference/table-operations/join/raj/) to keep the same left and right tables and match our timestamps as we'd expect.

For a `raj`, the values in the right table are matched to the closest values in the left table without going under the left value. For example, if the right table contains a value `5` and the left table contains values `4` and `6`, the right table's `5` will be matched on the left table's `4`.

Let's join these tables using a `raj`.

```python
joined_data_raj = daily_data_0.raj(daily_data_1, "DateTime", "Number1 = Number, Character1 = Character, Boolean1 = Boolean")
```

And now we have our table joined as expected.

