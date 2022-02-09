# Filter and decorate

In our previous notebook, we showed how to create some tables with fake data. In this notebook, we show how to decorate and filter our data.

Let's start by simulating measurements of our values every minute. This could represent something like stock prices, temperatures, etc.

```python
from deephaven.DateTimeUtils import currentTime, expressionToNanos, minus

time_interval = expressionToNanos("T1M")
offset = expressionToNanos("10D")
now = currentTime()

daily_data = create_random_table(time_interval, start_time=minus(now, offset))
```

Now we decorate the data by adding its day of the week.

```python
daily_data = daily_data.update("DayOfWeekInt = dayOfWeek(Timestamp, TZ_NY)")
```

Next, we convert the day of week to a string representation.

```python
import calendar

def day_of_week_int_to_str(day_of_week):
    return calendar.day_name[day_of_week-1]

daily_data = daily_data.update("DayOfWeekStr = day_of_week_int_to_str(DayOfWeekInt)")
```

Deephaven provides a wealth of [filtering methods for tables](https://deephaven.io/core/docs/how-to-guides/use-filters/). We start by filtering using simple boolean expressions.

```python
from deephaven.filter import or_

evens = daily_data.where("Number % 2 == 0")
odds = daily_data.where("!(Number % 2 == 0)")

trues = daily_data.where("Boolean")
falses = daily_data.where("!Boolean")

evens_and_trues = daily_data.where("Number % 2 == 0", "Boolean")
odds_or_falses = daily_data.where(or_("!(Number % 2 == 0)", "!Boolean"))
```

Some filtering methods can apply a filter to one table based on another.

```python
from deephaven.TableTools import newTable, stringCol

vowels_table = newTable(
    stringCol("Vowels", "A", "E", "I", "O", "U")
)

vowels = daily_data.whereIn(vowels_table, "Character = Vowels")
consonants = daily_data.whereNotIn(vowels_table, "Character = Vowels")
```

You can also define custom functions to perform filtering. Here, we omit weekend days.

```python
def is_weekday(day_of_week):
    return day_of_week <= 5 #Weekdays are 1 through 5

weekdays = daily_data.where("(boolean)is_weekday(DayOfWeekInt)")
weekends = daily_data.where("!((boolean)is_weekday(DayOfWeekInt))")
```

[The next notebook](A3%20Do%20time%20series%20and%20relational%20joins.md) will show how to perform joins on this data.
