# Filter and decorate

In this notebook, we show how to decorate and filter our data.

Let's start by simulating measurements of our values every minute. This could represent something like stock prices, temperatures, etc.

```python
from deephaven.time import now, to_nanos, minus

time_interval = to_nanos("00:01:00")
offset = to_nanos("240:00:00")
now_ = now()

daily_data = create_random_table(time_interval, start_time=minus(now_, offset))
```

Now we decorate the data by adding its day of the week.

```python
daily_data = daily_data.update(formulas=["DayOfWeekInt = dayOfWeek(Timestamp, TZ_NY)"])
```

Next, we convert the day of week to a string representation.

```python
import calendar

def day_of_week_int_to_str(day_of_week):
    return calendar.day_name[day_of_week-1]

daily_data = daily_data.update(formulas=["DayOfWeekStr = day_of_week_int_to_str(DayOfWeekInt)"])
```

Deephaven provides a wealth of [filtering methods for tables](https://deephaven.io/core/docs/how-to-guides/use-filters/). We start by filtering using simple boolean expressions.

```python
evens = daily_data.where(filters=["Number % 2 == 0"])
odds = daily_data.where(["!(Number % 2 == 0)"])

trues = daily_data.where(filters=["Boolean"])
falses = daily_data.where(filters=["!Boolean"])

evens_and_trues = daily_data.where(filters=["Number % 2 == 0", "Boolean"])
odds_or_falses = daily_data.where_one_of(filters=["!(Number % 2 == 0)", "!Boolean"])
```

Some filtering methods can apply a filter to one table based on another.

```python
from deephaven import new_table
from deephaven.column import string_col


vowels_table = new_table([
    string_col("Vowels", ["A", "E", "I", "O", "U"])
])

vowels = daily_data.where_in(filter_table=vowels_table, cols=["Character = Vowels"])
consonants = daily_data.where_not_in(filter_table=vowels_table, cols=["Character = Vowels"])
```

You can also define custom functions to perform filtering. Here, we omit weekend days.

```python
def is_weekday(day_of_week):
    return day_of_week <= 5 #Weekdays are 1 through 5

weekdays = daily_data.where(filters=["(boolean)is_weekday(DayOfWeekInt)"])
weekends = daily_data.where(filters=["!((boolean)is_weekday(DayOfWeekInt))"])
```

[The next notebook](A3%20Do%20time%20series%20and%20relational%20joins.md) will show how to perform joins on this data.
