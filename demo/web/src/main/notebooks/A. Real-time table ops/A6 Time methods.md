# Time methods

In our previous notebook, we showed how to use conditionals on time series data. In this notebook, we show how to use Deephaven's time methods on time series data.

Let's start again by simulating some real-time data, but this time with a shorter time interval.

```python
from deephaven.DateTimeUtils import currentTime, expressionToNanos, minus

time_interval = expressionToNanos("T1S")
offset = expressionToNanos("T1000S")
now = currentTime()

daily_data = create_random_table(time_interval, start_time=minus(now, offset))
```

## where

`where` filters can be applied to time series data. This example shows a few ways to filter your time series data.

```python
even_seconds = daily_data.where("secondOfMinute(Timestamp, TZ_NY) % 2 == 0")
odd_seconds = daily_data.where("secondOfMinute(Timestamp, TZ_NY) % 2 == 1")

even_minutes = daily_data.where("minuteOfHour(Timestamp, TZ_NY) % 2 == 0")
odd_minutes = daily_data.where("minuteOfHour(Timestamp, TZ_NY) % 2 == 1")
```

## update

With a ticking time table, you can add more columns by using `update`. These new columns are evaluated for every row in the table. If using a method to create the new columns, that method will be called for every row as well.

This example shows how to create new columns with the ticking time table.

```python
def random_fruit():
    fruits = [
        "apple",
        "pear",
        "orange",
        "grapefruit",
        "lime",
        "banana",
        "peach",
        "strawberry",
        "blueberry",
        "watermelon"
    ]

    return random.choice(fruits)

daily_data = daily_data.update("Fruit = random_fruit()")
```

## Datetime arithmethic

Deephaven supports datetime arithmetic through methods such as [plus](https://deephaven.io/core/docs/reference/time/datetime/plus/) and [minus](https://deephaven.io/core/docs/reference/time/datetime/minus/).

This example shows how to subtract 2 hours from a timestamp

```python
from deephaven.DateTimeUtils import convertPeriod

two_hours = convertPeriod("T2H")

daily_data = daily_data.update("TimestampTwoHoursBefore = minus(Timestamp, two_hours)")
```

## as-of joins

[as-of joins](https://deephaven.io/core/docs/reference/table-operations/join/aj/) allow you to join data without exact matches. This works very well with time-series data.

This example shows how to join two time series tables that have different densities in time stamps.

```python
time_interval = expressionToNanos("T10S")
offset = expressionToNanos("T1000S")
now = currentTime()

daily_data_two = create_random_table(time_interval, start_time=minus(now, offset))

joined_daily_data = daily_data.aj(daily_data_two, "Timestamp", "NumberTwo = Number, CharacterTwo = Character, BooleanTwo = Boolean")
```
