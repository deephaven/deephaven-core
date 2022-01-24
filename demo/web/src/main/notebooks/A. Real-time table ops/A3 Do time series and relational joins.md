# Time series and relational joins

In our previous notebook, we showed how to filter and decorate our time tables. In this notebook, we show how to perform joins with our time series data.

Let's start again by simulating a year's worth of daily measurements, but this time we will have two tables with slightly different timestamps. This is great for a simulation because there's no guarantee that a real example will collect data with exact timestamp matches.

```python
time_offset = Period("1D")

start_times = [
    convertDateTime("2020-01-01T00:00:00 NY"),
    convertDateTime("2020-01-01T00:00:02 NY")
]

daily_data_0 = create_random_table(365, start_times[0], time_offset)
daily_data_1 = create_random_table(365, start_times[1], time_offset)
```

To join these tables together based on the timestamps, we need to use an [as-of join](https://deephaven.io/core/docs/reference/table-operations/join/aj/). As-of joins, or `aj`, perform exact matches across all given columns except for the last one, which instead matches based on the closest values.

For an `aj`, the values in the right table are matched to the closest values in the left table without going over. This works well when the values in the left table are greater than the values in the right table.

Let's join these tables using an `aj` to get a single table with all of our information.

```python
joined_data_aj = daily_data_0.aj(daily_data_1, "DateTime", "Number1 = Number, Character1 = Character, Boolean1 = Boolean")
```

If you look at the `joined_data_aj` table, you may not see what you'd be expecting. Specifically, the first row won't have any values from our `daily_data_1` table, and the last row of the `daily_data_1` table isn't present. What happened?

Remember that an `aj` works based on a search where values in the right table are matched to the closest values in the left table without going over. If there's no matching key in the right table, the created row will have `NULL` values, and if there's no matching key in the left table, the row won't be included at all.

This is how we ended up with our table looking the way it is. When looking at the first row in our tables, the timestamp for the right table is `2020-01-01T00:00:02.000` and the timestamp for the left table is `2020-01-01T00:00:00.000`. Since these are the lowest values in our table, there's no match for the left table's timestamp that can be found without going over its current value (being `2020-01-01T00:00:00.000`).

This also explains how the last row in `daily_data_1` is lost. The timestamp of the last row in `daily_data_0` (`2020-12-30T00:00:00.000`) matches to the second to last row in `daily_data_1` (`2020-12-29T00:00:02.000`), leaving no row in the left table to match with the last row in the right table.

How can these tables join as expected? We could flip the left and right tables, but then our timestamp column will contain the values in `daily_data_1`, which are a bit messy. Instead, we can use a [reverse as-of join](https://deephaven.io/core/docs/reference/table-operations/join/raj/).

For a `raj`, the values in the right table are matched to the closest values in the left table without going under. This works well when the values in the left table are less than the values in the right table.

Let's join these tables using a `raj`.

```python
joined_data_raj = daily_data_0.raj(daily_data_1, "DateTime", "Number1 = Number, Character1 = Character, Boolean1 = Boolean")
```

That looks way better!
