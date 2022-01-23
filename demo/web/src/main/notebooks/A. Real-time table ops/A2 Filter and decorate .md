# Filter and decorate

Now that we have some data, let's work with it. In this notebook, we show how to decorate and filter our data.

Let's start by creating some daily data. This could simulate something like daily stock prices or daily temperatures.

```python
start_time = convertDateTime("2020-01-01T00:00:00 NY")

time_offset = Period("1D")
daily_data = create_random_table(365, start_time, time_offset)
```

Now we decorate the data by adding its day of the week.<!--TODO what is a good way to describe "decorate"?-->

```python
daily_data = daily_data.update("DayOfWeekInt = dayOfWeek(DateTime, TZ_NY)")
```

And now we convert the day of week to a string representation.

```python
import calendar

def day_of_week_int_to_str(day_of_week):
    return calendar.day_name[day_of_week-1]

daily_data = daily_data.update("DayOfWeekStr = day_of_week_int_to_str(DayOfWeekInt)")
```

Let's do some filtering on this data. For this example, we filter based on values in our columns with simple boolean expressions.

```python
from deephaven.filter import or_

evens = daily_data.where("Number % 2 == 0")
odds = daily_data.where("!(Number % 2 == 0)")

trues = daily_data.where("Boolean")
falses = daily_data.where("!Boolean")

evens_and_trues = daily_data.where("Number % 2 == 0", "Boolean")
odds_or_falses = daily_data.where(or_("!(Number % 2 == 0)", "!Boolean"))
```

For this example, we filter based on values in another table.

```python
from deephaven.TableTools import newTable, stringCol

vowels_table = newTable(
    stringCol("Vowels", "A", "E", "I", "O", "U")
)

vowels = daily_data.whereIn(vowels_table, "Character = Vowels")
consonants = daily_data.whereNotIn(vowels_table, "Character = Vowels")
```

For this example, we filter based on a custom function.

```python
def is_weekday(day_of_week):
    return day_of_week <= 5 #Weekdays are 1 through 5

weekdays = daily_data.where("(boolean)is_weekday(DayOfWeekInt)")
weekends = daily_data.where("!((boolean)is_weekday(DayOfWeekInt))")
```

[The next notebook](A3%20Do%20time%20series%20and%20relational%20joins.md) will show how to perform joins on this data.
