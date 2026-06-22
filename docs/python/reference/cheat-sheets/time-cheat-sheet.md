---
title: Time Operations Cheatsheet
sidebar_label: Time operations
---

Deephaven is a real-time data platform. Many of the applications that Deephaven is used for time-stamped data in some way, and this guide outlines the toolbox at your disposal. The guide on [working with time in Deephaven](../../conceptual/time-in-deephaven.md) provides conceptual detail that is not covered here.

## Date-time literals

The Deephaven engine understands values enclosed in _single quotes_ as date-time literals. These are useful in many places and are sprinkled throughout this document. Here are some examples:

```python order=time_literals_types,time_literals
from deephaven import empty_table

time_literals = empty_table(1).update(
    [
        "Instant = '2021-01-01T12:30:01.123 ET'",
        "Date = '2000-06-30'",
        "Time = '15:36:44.000'",
        "Timezone1 = 'CT'",
        "Timezone2 = 'Europe/London'",
        "Duration = 'PT36m15s'",
        "Period = 'P2y3m4d'",
    ]
)

time_literals_types = time_literals.meta_table
```

> [!NOTE]
> These should not be confused with strings, which are enclosed in _backticks_.

## Create tables with timestamps

### `new_table`

[`new_table`](../../reference/table-operations/create/newTable.md) creates a Deephaven table from Python objects:

```python
from deephaven import new_table
from deephaven.column import datetime_col
import datetime as dt
import pandas as pd
import numpy as np

# Note the many different kinds of Python date-time objects you can use
t_new_table = new_table(
    [
        datetime_col(
            "Timestamp",
            [
                dt.datetime(2023, 6, 1),
                np.datetime64("2023-06-02"),
                pd.Timestamp(2023, 6, 3),
                dt.date(2023, 6, 4),
                *pd.DatetimeIndex(["2023-06-05", "2023-06-06", "2023-06-07"]),
                *np.arange(
                    np.datetime64("2023-06-08"),
                    np.datetime64("2023-06-11"),
                    np.timedelta64(1, "D"),
                ),
            ],
        )
    ]
)
```

### `empty_table`

To use the query language instead of Python to create a timestamped table, use [`empty_table`](../../reference/table-operations/create/emptyTable.md) and [`update`](../../reference/table-operations/select/update.md):

```python
from deephaven import empty_table

t_empty_table = empty_table(10).update(
    ["Timestamp = '2023-06-01T00:00:00 UTC' + 'PT24h'.multipliedBy(ii)"]
)
```

### `time_table`

Lastly, [`time_table`](../../reference/table-operations/create/timeTable.md) is good for creating a ticking table:

```python ticking-table order=null
from deephaven import time_table

t_time_table = time_table("PT1s")
```

![The above `t_time_table` ticking in the Deephaven console](../../assets/reference/cheat-sheets/cheat-sheet-1.gif)

## Parse time-stamped data

### Parse date-times

When timestamp columns contain strings formatted to [ISO 8641 standards](https://en.wikipedia.org/wiki/ISO_8601), the [`parseInstant`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseInstant(java.lang.String)) function can be used to convert them to the [`Instant`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/Instant.html) date-time type:

```python test-set=1 order=t1,t1_raw,t1_meta,t1_raw_meta
from deephaven import new_table
from deephaven.column import string_col

t1_raw = new_table(
    [
        string_col(
            "Timestamp",
            [
                "2023-01-01T12:30:01.00 CT",
                "2023-01-01T12:30:02.00 CT",
                "2023-01-01T12:30:03.00 CT",
            ],
        )
    ]
)

t1 = t1_raw.update("NewTimestamp = parseInstant(Timestamp)")

t1_raw_meta = t1_raw.meta_table
t1_meta = t1.meta_table
```

Often, date-time formats do not adhere to strict ISO standards, so they must be massaged before they can be used as a proper date-time type.
This is done with the [`simple_date_format`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) function from the [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) library:

```python test-set=1 order=t2,t2_raw,t2_meta,t2_raw_meta
from deephaven import new_table
from deephaven.column import string_col
from deephaven.time import simple_date_format

t2_raw = new_table(
    [
        string_col(
            "Timestamp",
            [
                "01/01/2023 12:30:01 CT",
                "01/01/2023 12:30:02 CT",
                "01/01/2023 12:30:03 CT",
            ],
        )
    ]
)

# Describe the existing format to simple_date_format, and it can perform the conversion
input_format = simple_date_format("MM/dd/yyyy HH:mm:ss z")

# The timezone "CT" gets replaced with "CST" because simple_date_format.parse() does not understand "CT"
t2 = t2_raw.update(
    "NewTimestamp = input_format.parse(Timestamp.replaceAll(`CT`, `CST`)).toInstant()"
)

t2_raw_meta = t2_raw.meta_table
t2_meta = t2.meta_table
```

In this example, the timezone can be interpreted automatically, eliminating the need for string replacement:

```python test-set=1 order=t3,t3_raw,t3_meta,t3_raw_meta
from deephaven import new_table
from deephaven.column import string_col
from deephaven.time import simple_date_format

t3_raw = new_table(
    [
        string_col(
            "Timestamp",
            ["20230101-12:30:01 CST", "20230101-12:30:02 CST", "20230101-12:30:03 CST"],
        )
    ]
)

input_format = simple_date_format("yyyyMMdd-HH:mm:ss z")

t3 = t3_raw.update("NewTimestamp = input_format.parse(Timestamp).toInstant()")

t3_raw_meta = t3_raw.meta_table
t3_meta = t3.meta_table
```

Here, the dates and times are stored in separate columns. Extract the data from each column and use [`parseInstant`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseInstant(java.lang.String)) or [`toInstant`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#toInstant(java.time.LocalDate,java.time.LocalTime,java.time.ZoneId)) to get the final result:

```python test-set=1 order=t4,t4_raw,t4_meta,t4_raw_meta
from deephaven import new_table
from deephaven.column import string_col

t4_raw = new_table(
    [
        string_col("Date", ["2023-01-01", "2023-01-01", "2023-01-01"]),
        string_col("Time", ["12:30:01 CT", "12:30:02 CT", "12:30:03 CT"]),
    ]
)

# parseLocalDate, parseLocalTime, and parseTimeZone are used to convert strings to the correct Java types
t4 = t4_raw.update(
    [
        # First concatenate strings, then use parseInstant()
        "NewTimestamp1 = parseInstant(Date + `T` + Time)",
        # Alternatively, parse each component into correct type, then use toInstant
        "NewTimestamp2 = toInstant(parseLocalDate(Date), \
        parseLocalTime(Time.split(` `)[0]), parseTimeZone(Time.split(` `)[1]))",
    ]
)

t4_raw_meta = t4_raw.meta_table
t4_meta = t4.meta_table
```

Java methods are often useful for getting things into the required format. Here, the [`LocalDate.of`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/LocalDate.html#of(int,int,int)) method is used to create a [`LocalDate`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/LocalDate.html) that is then fed to [`to_instant`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#toInstant(java.time.LocalDate,java.time.LocalTime,java.time.ZoneId)):

```python test-set=1 order=t5,t5_raw,t5_meta,t5_raw_meta
from deephaven import new_table
from deephaven.column import string_col, int_col

t5_raw = new_table(
    [
        int_col("Month", [1, 1, 1]),
        int_col("Day", [1, 1, 1]),
        int_col("Year", [2023, 2023, 2023]),
        string_col("Time", ["12:30:01", "12:30:02", "12:30:03"]),
        string_col("Timezone", ["CT", "CT", "CT"]),
    ]
)

# The timezone is defined explicitly because parseTimeZone does not understand "CST"
t5 = t5_raw.update(
    "NewTimestamp = toInstant(LocalDate.of(Year, Month, Day), parseLocalTime(Time), 'CT')"
)

t5_raw_meta = t5_raw.meta_table
t5_meta = t5.meta_table
```

### Parse dates

When a date column is a string with proper ISO formatting, [`parseLocalDate`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseLocalDate(java.lang.String)) can be used to convert the column to the correct type:

```python test-set=1 order=t6,t6_raw,t6_meta,t6_raw_meta
from deephaven import new_table
from deephaven.column import string_col

t6_raw = new_table([string_col("Date", ["2023-01-01", "2023-01-02", "2023-01-03"])])

t6 = t6_raw.update("NewDate = parseLocalDate(Date)")

t6_raw_meta = t6_raw.meta_table
t6_meta = t6.meta_table
```

When the format is unknown, [`simple_date_format`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) is useful for converting to the appropriate format:

```python test-set=1 order=t7,t7_raw,t7_meta,t7_raw_meta
from deephaven import new_table
from deephaven.column import string_col
from deephaven.time import simple_date_format

t7_raw = new_table([string_col("Date", ["1/1/23", "1/2/23", "1/3/23"])])

input_format = simple_date_format("MM/dd/yy")

t7 = t7_raw.update("NewDate = toLocalDate(input_format.parse(Date).toInstant(), 'UTC')")

t7_raw_meta = t7_raw.meta_table
t7_meta = t7.meta_table
```

Here, the date is presented as an integer, so it must be cast to a String before using [`simple_date_format`](/core/pydoc/code/deephaven.time.html#module-deephaven.time):

```python test-set=1 order=t8,t8_raw,t8_meta,t8_raw_meta
from deephaven import new_table
from deephaven.column import int_col
from deephaven.time import simple_date_format

t8_raw = new_table([int_col("Date", [230101, 230102, 230103])])

input_format = simple_date_format("yyMMdd")

t8 = t8_raw.update(
    "NewDate = toLocalDate(input_format.parse(String.valueOf(Date)).toInstant(), 'UTC')"
)

t8_raw_meta = t8_raw.meta_table
t8_meta = t8.meta_table
```

### Parse times

Much like dates, string columns containing ISO-formatted times should use [`parseLocalTime`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseLocalTime(java.lang.String)):

```python test-set=1 order=t9,t9_raw,t9_meta,t9_raw_meta
from deephaven import new_table
from deephaven.column import string_col

t9_raw = new_table([string_col("Time", ["12:30:00", "12:30:01", "12:30:02"])])

t9 = t9_raw.update("NewTime = parseLocalTime(Time)")

t9_raw_meta = t9_raw.meta_table
t9_meta = t9.meta_table
```

When the format is unknown, [`simple_date_format`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) is still useful:

```python test-set=1 order=t10,t10_raw,t10_meta,t10_raw_meta
from deephaven import new_table
from deephaven.column import string_col
from deephaven.time import simple_date_format

t10_raw = new_table([string_col("Time", ["12.30.00", "12.30.01", "12.30.02"])])

input_format = simple_date_format("HH.mm.ss")

t10 = t10_raw.update(
    "NewTime = toLocalTime(input_format.parse(Time).toInstant(), 'UTC')"
)

t10_raw_meta = t10_raw.meta_table
t10_meta = t10.meta_table
```

Times represented with no delimiters may be stored as integers, so they must be converted to Strings first:

```python test-set=1 order=t11,t11_raw,t11_meta,t11_raw_meta
from deephaven import new_table
from deephaven.column import string_col
from deephaven.time import simple_date_format

t11_raw = new_table([int_col("Time", [123000, 123001, 123002])])

input_format = simple_date_format("HHmmss")

t11 = t11_raw.update(
    "NewTime = toLocalTime(input_format.parse(String.valueOf(Time)).toInstant(), 'UTC')"
)

t11_raw_meta = t11_raw.meta_table
t11_meta = t11.meta_table
```

Alternatively, integers can be interpreted as seconds since the [Unix Epoch](https://en.wikipedia.org/wiki/Unix_time):

```python test-set=1
t11_epoch = t11_raw.update("Time = epochSecondsToInstant(Time)")
```

## Reformat time-stamped data

The [`format_columns`](../../reference/table-operations/format/format-columns.md) table operation can be used to change the UI-level formatting of a date-time column. Note that this has no effect on the internal representation of the data, only on the way that data is presented. Reformat timestamps using [valid Java date-time formats](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/format/DateTimeFormatter.html):

```python test-set=2 order=t1,t2,t3,t4,t5,t6
from deephaven import empty_table

t1 = empty_table(10).update("Timestamp = '2021-01-01T12:30:00Z' + i * 'P1d'")

# double digits for day and month, separated by spaces
t2 = t1.format_columns("Timestamp = Date(`dd MM y HH:mm:ss`)")

# separate year, month, and day by dashes
t3 = t1.format_columns("Timestamp = Date(`y-MM-dd HH:mm:ss`)")

# spell out the month explicitly
t4 = t1.format_columns("Timestamp = Date(`LLL d, y HH:mm:ss`)")

# spell out day and month explicitly
t5 = t1.format_columns("Timestamp = Date(`EEE, LLL d, y HH:mm:ss`)")

# only day of month and time
t6 = t1.format_columns("Timestamp = Date(`dd HH:mm:ss`)")
```

t = time_table("PT1S").format_columns("Timestamp = Date(`HH:mm:ss.SSSSSSSSS`)")

## Get current time information

### In a query

```python test-set=3
from deephaven import empty_table

t = empty_table(1).update(
    ["CurrentTime = now()", "CurrentDate = today()", "CurrentTimeZone = timeZone()"]
)
```

### Outside of a query

```python test-set=3
from deephaven.time import dh_now, dh_today, dh_time_zone

print("Current time: ", dh_now())
print("Current date:", dh_today())
print("Current time zone: ", dh_time_zone())
```

## Time zones

The time zone of the Deephaven engine and the time zone displayed by the UI are not always identical. This section covers time zone operations with respect to the engine's time zone. The UI section will cover time zones in the UI.

### Change engine time zone

The engine time zone is set on server startup and cannot be modified once the server has been started. To change the engine time zone, see [the Community Question on setting timezone](../../reference/community-questions/set-timezone.md).

### Time zone aliases

Deephaven provides time zone aliases for some common time zones:

```python test-set=4
from deephaven import empty_table

zones2 = empty_table(1).update(["EasternTimeZone = 'ET'", "PacificTimeZone = 'PT'"])
```

For all other time zones, you can create custom time zone aliases with [`time_zone_alias_add`](/core/pydoc/code/deephaven.time.html#deephaven.time.time_zone_alias_add) and use them in queries:

```python test-set=4
from deephaven.time import time_zone_alias_add, time_zone_alias_rm
from deephaven import empty_table

# Add custom time zone alias
time_zone_alias_add("WST", "Pacific/Samoa")

zones3 = empty_table(1).update(
    [
        "EasternTimeZone = 'ET'",
        "PacificTimeZone = 'PT'",
        "SamoaTimeZone1 = 'Pacific/Samoa'",
        "SamoaTimeZone2 = 'WST'",
        # Timezone aliases will get recognized by parsing methods
        "EasternTimeInstant = parseInstant(`2022-04-06T12:50:43 ET`)",
        "PacificTimeInstant = parseInstant(`2022-04-06T12:50:43 PT`)",
        # Custom aliases are also understood
        "SamoaTimeInstant = parseInstant(`2022-04-06T12:50:43 WST`)",
    ]
)

# Remove custom time zone alias, further queries using "WST" will fail
time_zone_alias_rm("WST")
```

## Manipulate timestamps

The following table will be used to demonstrate some common operations:

```python test-set=5
from deephaven import empty_table

t = empty_table(2 * 365 * 24 * 60).update(
    [
        "Timestamp = '2020-01-01T00:00:00.00 UTC' + 'PT1m'.multipliedBy(ii)",
        "X = randomGaussian(0, 1)",
    ]
)
```

### Convenience methods for date-times

Date-time columns carry a lot of information, which can be extracted with Deephaven's [auto-imported functions](../../reference/query-language/query-library/auto-imported/index.md):

```python test-set=5
t_more_methods = t.update(
    [
        "DateChicago = toLocalDate(Timestamp, 'America/Chicago')",
        "MinuteOfDayChicago = minuteOfDay(Timestamp, 'America/Chicago', false)",
        "HourOfDayChicago = hourOfDay(Timestamp, 'America/Chicago', false)",
        "DayOfWeekChicago = dayOfWeek(Timestamp, 'America/Chicago')",
        "DayOfMonthChicago = dayOfMonth(Timestamp, 'America/Chicago')",
        "DayOfYearChicago = dayOfYear(Timestamp, 'America/Chicago')",
        "DateTokyo = toLocalDate(Timestamp, 'Asia/Tokyo')",
        "MinuteOfDayTokyo = minuteOfDay(Timestamp, 'Asia/Tokyo', false)",
        "HourOfDayTokyo = hourOfDay(Timestamp, 'Asia/Tokyo', false)",
        "DayOfWeekTokyo = dayOfWeek(Timestamp, 'Asia/Tokyo')",
        "DayOfMonthTokyo = dayOfMonth(Timestamp, 'Asia/Tokyo')",
        "DayOfYearTokyo = dayOfYear(Timestamp, 'Asia/Tokyo')",
        "SecondsSinceEpoch = epochSeconds(Timestamp)",
    ]
)
```

### Bin timestamps

The [`upperBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long)) and [`lowerBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)) functions are useful for creating timestamp bins in regular intervals. Each method can accept a time interval as a number of nanoseconds (e.g., `5 * SECOND`) or a [Duration](../../reference/query-language/types/durations.md) (e.g., `'PT5S'`):

```python test-set=5 order=t_binned
t_binned = t.update(
    [
        "BinnedTimestamp = lowerBin(Timestamp, 'PT10m')",
    ]
)
```

These bins can be used along with some table operations for downsampling:

```python test-set=5 order=t_binned_first,t_binned_last,t_binned_avg
# Downsample to first observation every 10 minutes
t_binned_first = t_binned.first_by("BinnedTimestamp")

# Downsample to last observation every 10 minutes
t_binned_last = t_binned.last_by("BinnedTimestamp")

# Downsample to average every 10 minutes
t_binned_avg = t_binned.select(["X", "BinnedTimestamp"]).avg_by("BinnedTimestamp")
```

Or, the [`agg`](/core/pydoc/code/deephaven.agg.html#module-deephaven.agg) suite can perform multiple aggregations at once on binned timestamps:

```python test-set=5
from deephaven import agg

t_ohlc = t_binned.agg_by(
    [agg.first("Open=X"), agg.max_("High=X"), agg.min_("Low=X"), agg.last("Close=X")],
    by="Timestamp",
)
```

### Time-based filtering

Deephaven's [filter methods](../../how-to-guides/use-filters.md) and [logical operators](../../how-to-guides/operators.md#logical-operators) can be used with timestamps:

```python test-set=5 order=t_2020,t_2021,t_2021_summer
# Compare the Timestamp column to an exact timestamp literal
t_2020 = t.where("Timestamp < '2021-01-01T00:00:00.00 UTC'")

# Use the built-in function year() in a filter
t_2021 = t.where("year(Timestamp, 'UTC') == 2021")

# Built-in functions, methods, literals, and multiple filters
t_2021_summer = t.where(
    ["Timestamp >= atMidnight('2021-01-01', 'UTC').toInstant()"]
).where_one_of(
    [
        "monthOfYear(Timestamp, 'UTC') == 6",
        "monthOfYear(Timestamp, 'UTC') == 7",
        "monthOfYear(Timestamp, 'UTC') == 8",
    ]
)
```

### Math with date-times

Mathematical operations on timestamps are well-defined:

```python test-set=5
# Many operations have nanosecond resolution, so conversion to the desired unit is often required
t_with_math = t.update(
    [
        "OneYearAgo = Timestamp - 'P365d'",
        "OneYearNanos = Timestamp - OneYearAgo",
        "OneYear = OneYearNanos / YEAR_365",
        "DaysSinceTimestamp = (now() - Timestamp) / DAY",
    ]
)
```

The results can be used in filters without constructing new columns:

```python test-set=5
# Get data from less than 1000 days ago
t_recent = t.where("('2024-04-03T15:30:00.00 UTC' - Timestamp) / DAY < 1000")
```

### Built-in constants

The previous examples utilize our [built-in constants](/core/javadoc/io/deephaven/time/DateTimeUtils.html) `YEAR_365` and `DAY`. Deephaven offers many such constants to help with unit conversion:

| Constant                                                                                                    | Type        | Description                                                       |
| ----------------------------------------------------------------------------------------------------------- | ----------- | ----------------------------------------------------------------- |
| [`DAY`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#DAY)                                             | `long`      | One day in nanoseconds.                                           |
| [`DAYS_PER_NANO`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#DAYS_PER_NANO)                         | `double`    | Number of days per nanosecond.                                    |
| [`HOUR`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#HOUR)                                           | `long`      | One hour in nanoseconds.                                          |
| [`HOURS_PER_NANO`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#HOURS_PER_NANO)                       | `double`    | Number of hours per nanosecond.                                   |
| [`MICRO`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#MICRO)                                         | `long`      | One microsecond in nanoseconds.                                   |
| [`MILLI`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#MILLI)                                         | `long`      | One millisecond in nanoseconds.                                   |
| [`MINUTE`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#MINUTE)                                       | `long`      | One minute in nanoseconds.                                        |
| [`MINUTES_PER_NANO`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#MINUTES_PER_NANO)                   | `double`    | Number of minutes per nanosecond.                                 |
| [`SECOND`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#SECOND)                                       | `long`      | One second in nanoseconds.                                        |
| [`SECONDS_PER_NANO`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#SECONDS_PER_NANO)                   | `double`    | Number of seconds per nanosecond.                                 |
| [`WEEK`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#WEEK)                                           | `long`      | One week in nanoseconds.                                          |
| [`YEAR_365`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#YEAR_365)                                   | `long`      | One 365 day year in nanoseconds.                                  |
| [`YEAR_AVG`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#YEAR_AVG)                                   | `long`      | One average year in nanoseconds.                                  |
| [`YEARS_PER_NANO_365`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#YEARS_PER_NANO_365)               | `double`    | Number of 365 day years per nanosecond nanosecond.                |
| [`YEARS_PER_NANO_AVG`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#YEARS_PER_NANO_AVG)               | `double`    | Number of average (365.2425 day) years per nanosecond nanosecond. |
| [`ZERO_LENGTH_INSTANT_ARRAY`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#ZERO_LENGTH_INSTANT_ARRAY) | `Instant[]` | A zero length array of instants.                                  |

## Time-based joins

Deephaven offers powerful table operations for joining time-stamped data. In practice, timestamps across multiple datasets are rarely exactly the same, so attempts to join on timestamps with traditional join methods will fail. To this end, Deephaven offers the [`aj`](../../reference/table-operations/join/aj.md) and [`raj`](../../reference/table-operations/join/raj.md) time-based joins.

### `aj`(as-of join)

The as-of join [`aj`](../../reference/table-operations/join/aj.md) joins tables on timestamp columns by choosing values from the right key column that are as close as possible to values in the left key column _without going over_:

```python test-set=5 order=as_of_joined,t_noisy
t_noisy = empty_table(2 * 365 * 24 * 60).update(
    [
        "Timestamp = '2020-01-01T00:00:00.00 UTC' + 'PT1m'.multipliedBy(ii) + 'PT0.1s'.multipliedBy(randomInt(0, 10*60-1))",
        "Y = randomGaussian(0, 1)",
    ]
)

as_of_joined = t.aj(t_noisy, on="Timestamp", joins="Y")
```

[`aj`](../../reference/table-operations/join/aj.md) also works when one or more key columns includes exact matches:

```python test-set=5 order=as_of_joined_exact,t_group,t_noisy_group
t_group = t.update("Group = randomInt(0, 5)")
t_noisy_group = t_noisy.update("Group = randomInt(0, 5)")

as_of_joined_exact = t_group.aj(t_noisy_group, on=["Group", "Timestamp"], joins="Y")
```

### `raj`(reverse-as-of join)

The reverse-as-of join [`raj`](../../reference/table-operations/join/raj.md) joins tables on timestamp columns by choosing values from the right key column that are as close as possible to values in the left key column _without going under_:

```python test-set=5 order=reverse_as_of_joined,t_noisy_timestamps
t_noisy_timestamps = empty_table(2 * 365 * 24 * 60).update(
    [
        "Timestamp = '2020-01-01T00:00:00.00 UTC' + 'PT1m'.multipliedBy(ii) + 'PT0.1s'.multipliedBy(randomInt(0, 10*60-1))",
        "Y = randomGaussian(0, 1)",
    ]
)

reverse_as_of_joined = t.raj(t_noisy_timestamps, on="Timestamp", joins="Y")
```

Just like [`aj`](../../reference/table-operations/join/aj.md), [`raj`](../../reference/table-operations/join/raj.md) works when one or more key columns includes exact matches:

```python test-set=5 order=reverse_as_of_joined_exact,t_group,t_noisy_group
t_group = t.update("Group = randomInt(0, 5)")
t_noisy_group = t_noisy.update("Group = randomInt(0, 5)")

reverse_as_of_joined_exact = t_group.raj(
    t_noisy_group, on=["Group", "Timestamp"], joins="Y"
)
```

## Work with Python time types

Python supports date-time types through the [`datetime`](https://docs.python.org/3/library/datetime.html), [`numpy`](https://numpy.org/doc/stable/reference/arrays.datetime.html), and [`pandas`](https://pandas.pydata.org/docs/user_guide/timeseries.html) libraries. Deephaven provides interoperability with these types through the [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) library.

### Python to Java

The [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) library provides the following functions for converting Python types from the aformentioned libraries to Java types for use in query strings:

- [`to_j_time_zone`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_time_zone)
- [`to_j_local_date`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_date)
- [`to_j_local_time`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_time)
- [`to_j_instant`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_instant)
- [`to_j_zdt`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_zdt)
- [`to_j_duration`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_duration)
- [`to_j_period`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_period)

> [!WARNING]
> For performance reasons, these functions should _not_ be called _inside_ of query strings. They should be called to create new objects _outside_ of query strings, and those objects may then be used inside of query strings.

This example demonstrates the proper use of these functions:

```python test-set=6 order=t,t_meta
import datetime as dt
import numpy as np
import pandas as pd
import deephaven.time as dhtu
from deephaven import empty_table

my_tz = dhtu.to_j_time_zone(dt.timezone(offset=-dt.timedelta(hours=5)))
my_date = dhtu.to_j_local_date(np.datetime64("2021-07-29"))
my_time = dhtu.to_j_local_time(pd.Timestamp(2023, 1, 2, 7, 44, 31))
my_datetime = dhtu.to_j_instant(dt.datetime(2024, 4, 30, 12, 40, 32))
my_zdt = dhtu.to_j_zdt(pd.Timestamp(2020, 3, 15, 12, 28, 45, tz="America/Chicago"))
my_duration = dhtu.to_j_duration(np.timedelta64(24, "m"))
my_period = dhtu.to_j_period(dt.timedelta(days=17))

t = empty_table(1).update(
    [
        "ZoneIdCol = my_tz",
        "LocalDateCol = my_date",
        "LocalTimeCol = my_time",
        "InstantCol = my_datetime",
        "ZDTCol = my_zdt",
        "DurationCol = my_duration",
        "PeriodCol = my_period",
    ]
)

t_meta = t.meta_table
```

These functions all accept multiple argument types, enabling conversion from any of the Python types to the relevant Java types.

### Java to Python

Similarly, [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) offers the following functions to convert Java types to their Python equivalents:

- [`to_date`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_date)
- [`to_time`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_time)
- [`to_datetime`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_datetime)
- [`to_timedelta`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_timedelta)
- [`to_np_datetime64`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_datetime64)
- [`to_np_timedelta64`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_timedelta64)
- [`to_pd_timestamp`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timestamp)
- [`to_pd_timedelta`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timedelta)

These are often useful when calling a Python function from a query string that takes a date-time type as an argument:

```python test-set=7
import datetime as dt
import deephaven.time as dhtu
from deephaven import empty_table


def seconds_between(time1, time2) -> float:
    dt_time1 = dhtu.to_datetime(time1)
    dt_time2 = dhtu.to_datetime(time2)

    return abs((dt_time1 - dt_time2).total_seconds())


t = empty_table(10).update(
    [
        "Timestamp1 = now()",
        "Timestamp2 = '2023-05-03T15:45:03.22Z' + ii * DAY",
        "SecondsBetween = seconds_between(Timestamp1, Timestamp2)",
    ]
)
```

> [!NOTE]
> The query language methods would be better for this particular use case, but this flexibility enables solving problems that the query langauge does not have solutions for.

## Date-time settings in the UI

The Settings menu in the Deephaven IDE offers a few ways to customize the appearance of date-time types:

![The Deephaven IDE's **Settings** menu](../../assets/reference/cheat-sheets/ide-settings.png)

Here, you can select a time zone in which to view data. This time zone will apply to all date-time columns in all tables. You can also change the format of date-time columns, which may be useful for larger tables. Also, the `Format by Column Name & Type` can be used to format specific columns, enabling you apply a time zone to specific columns in a table.
