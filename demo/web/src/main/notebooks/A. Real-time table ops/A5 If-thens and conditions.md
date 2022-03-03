# If-then and conditions

In this notebook, we show how to use if-then statements and conditionals in Deephaven.

Let's start again by simulating some real-time data.

```python
from deephaven.DateTimeUtils import currentTime, expressionToNanos, minus

time_interval = expressionToNanos("1D")
offset = expressionToNanos("1000D")
now = currentTime()

daily_data = create_random_table(time_interval, start_time=minus(now, offset))
```

## Ternaries

Deephaven supports [Java ternaries](https://deephaven.io/core/docs/reference/query-language/control-flow/ternary-if/). We can use these to perform if-else statements on our tables to assign values to a new column based on the if-else evaluation.

```python
daily_data = daily_data.update("IsEven = Number%2 == 0 ? true : false")
```

This query creates a new column `IsEven` in the `daily_data` table.

### Custom methods for ternary conditions

You may want to use a custom method for your condition for your ternary. To do this, you simply use the method instead of the hardcoded conditional. There is a catch - you need to cast the method to `(Boolean)` for Deephaven to recognize it properly.

Let's use the `is_weekday` method in a ternary statement to create a new column that tells us if a given day falls during the week or weekend.

```python
def is_weekday(day_of_week):
    return day_of_week <= 5

daily_data = daily_data.update("DayOfWeekInt = dayOfWeek(Timestamp, TZ_NY)")\
    .update("IsWeekday = (Boolean)is_weekday(DayOfWeekInt) ? true: false")
```
