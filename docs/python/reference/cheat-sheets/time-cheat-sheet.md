---
title: Time operations cheat sheet
---

Deephaven is a real-time data platform, and many Deephaven applications use time-stamped data in some way. This cheat sheet outlines the tools available for working with that data. The guide on [working with time in Deephaven](../../conceptual/time-in-deephaven.md) provides conceptual detail that this cheat sheet does not cover.

## Date-time literals

In query strings, a value of more than one character in _single quotes_ is a date-time literal. The engine parses it as an [instant](../query-language/types/date-time.md#instant), a date, a time, a time zone, a [duration](../query-language/types/durations.md), or a [period](../query-language/types/periods.md). [Date-time literals in query strings](../../how-to-guides/date-time-literals.md) lists the formats for each type. Short zone names such as `ET` and `CT` are Deephaven [time zone aliases](#time-zone-aliases). The following example creates one column of each kind. Its [`meta_table`](../table-operations/metadata/meta_table.md), `time_literals_types`, shows each column's type:

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
> Date-time literals use single quotes. Strings in query strings use _backticks_.

## Create tables with timestamps

### `new_table`

[`new_table`](../table-operations/create/newTable.md) and [`datetime_col`](../table-operations/create/dateTimeCol.md) create a table with an [`Instant`](../query-language/types/date-time.md#instant) column from Python date-time objects:

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

To use the query language instead of Python to create a timestamped table, use [`empty_table`](../table-operations/create/emptyTable.md) and [`update`](../table-operations/select/update.md):

```python
from deephaven import empty_table

t_empty_table = empty_table(10).update(
    ["Timestamp = '2023-06-01T00:00:00 UTC' + 'PT24h'.multipliedBy(ii)"]
)
```

### `time_table`

[`time_table`](../table-operations/create/timeTable.md) creates a [ticking table](../../how-to-guides/time-table.md), which adds a new row at a fixed interval. It takes the interval in any of these forms:

- A [duration](../query-language/types/durations.md) string, such as `"PT1s"` or `"PT00:00:00.001"`.
- An integer number of nanoseconds.
- A Python time duration type, such as `datetime.timedelta`.

The optional `start_time` argument sets the start time. When the start time is in the past, the table immediately contains one row for each interval between the start time and the current time:

```python ticking-table order=null
from deephaven import time_table

# Start now and add a row every second
t_time_table = time_table("PT1s")

# Start at a fixed time and add a row every hour
t_time_table_start = time_table("PT1h", start_time="2024-01-01T00:00:00 ET")
```

![The `t_time_table` table ticking in the Deephaven IDE, adding one row each second](../../assets/reference/cheat-sheets/cheat-sheet-1.gif)

## Parse time-stamped data

Each table example in this section converts a raw table and also shows the [`meta_table`](../table-operations/metadata/meta_table.md) of both the raw and the converted table, so the column types can be compared before and after the conversion.

### Parse date-times

[`parseInstant`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseInstant(java.lang.String)) converts strings in an [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601)-style format to the [`Instant`](../query-language/types/date-time.md#instant) date-time type. The format is `yyyy-MM-ddTHH:mm:ss[.SSSSSSSSS] TZ`, where `TZ` is a time zone name or alias:

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

[`parseInstant`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseInstant(java.lang.String)) accepts only ISO 8601-style date-times and plain integer offsets from the Unix epoch, so it cannot parse custom layouts such as `01/01/2023 12:30:01 CT`. To parse them, describe their format to [`simple_date_format`](../time/datetime/simple_date_format.md) from the [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) module, which returns a Java [`SimpleDateFormat`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/SimpleDateFormat.html):

```python test-set=1 order=t2,t2_raw,t2_meta,t2_raw_meta
from deephaven import new_table
from deephaven.column import string_col
from deephaven.time import simple_date_format
from deephaven.table import Selectable

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

# Replace the time zone "CT" with "CST" because SimpleDateFormat.parse() does not understand "CT"
# with_serial() keeps the engine from using the shared input_format on several threads at once
t2 = t2_raw.update(
    Selectable.parse(
        "NewTimestamp = input_format.parse(Timestamp.replaceAll(`CT`, `CST`)).toInstant()"
    ).with_serial()
)

t2_raw_meta = t2_raw.meta_table
t2_meta = t2.meta_table
```

> [!WARNING]
> A `SimpleDateFormat` is not thread-safe. On a large table, the engine can run a [formula](../../how-to-guides/formulas.md) on several threads at once, and a shared `SimpleDateFormat` can then return wrong values. The example above prevents this. It passes the formula to [`Selectable.parse`](/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable) and marks the column serial with [`with_serial`](../../conceptual/query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where), so the engine never runs the formula concurrently with itself. The later examples on this page that use [`simple_date_format`](../time/datetime/simple_date_format.md) need the same change on large tables.

In this example, the strings use a different layout and already carry the abbreviation `CST`. `SimpleDateFormat` recognizes `CST`, so the query does not call `replaceAll`:

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

Here, the dates and times are stored in separate columns. Extract the data from each column and use [`parseInstant`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseInstant(java.lang.String)) or [`toInstant`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#toInstant(java.time.LocalDate,java.time.LocalTime,java.time.ZoneId)) to get the final result:

```python test-set=1 order=t4,t4_raw,t4_meta,t4_raw_meta
from deephaven import new_table
from deephaven.column import string_col

t4_raw = new_table(
    [
        string_col("Date", ["2023-01-01", "2023-01-01", "2023-01-01"]),
        string_col("Time", ["12:30:01 CT", "12:30:02 CT", "12:30:03 CT"]),
    ]
)

t4 = t4_raw.update(
    [
        # First concatenate strings, then use parseInstant()
        "NewTimestamp1 = parseInstant(Date + `T` + Time)",
        # Alternatively, parse each component with parseLocalDate, parseLocalTime, and parseTimeZone, then use toInstant
        "NewTimestamp2 = toInstant(parseLocalDate(Date), "
        "parseLocalTime(Time.split(` `)[0]), parseTimeZone(Time.split(` `)[1]))",
    ]
)

t4_raw_meta = t4_raw.meta_table
t4_meta = t4.meta_table
```

Java methods can also build the arguments that [`toInstant`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#toInstant(java.time.LocalDate,java.time.LocalTime,java.time.ZoneId)) takes. Here, [`LocalDate.of`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html#of(int,int,int)) creates a [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html), and the example passes it to [`toInstant`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#toInstant(java.time.LocalDate,java.time.LocalTime,java.time.ZoneId)):

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

t5 = t5_raw.update(
    "NewTimestamp = toInstant(LocalDate.of(Year, Month, Day), parseLocalTime(Time), parseTimeZone(Timezone))"
)

t5_raw_meta = t5_raw.meta_table
t5_meta = t5.meta_table
```

Integers can hold an offset from the [Unix epoch](https://en.wikipedia.org/wiki/Unix_time). [`epochSecondsToInstant`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochSecondsToInstant(long)) converts a count of seconds to an [`Instant`](../query-language/types/date-time.md#instant). [`epochAutoToInstant`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochAutoToInstant(long)) infers whether the offset is in seconds, milliseconds, microseconds, or nanoseconds from its magnitude:

```python test-set=1 order=t_epoch,t_epoch_raw,t_epoch_meta,t_epoch_raw_meta
from deephaven import new_table
from deephaven.column import long_col

t_epoch_raw = new_table(
    [long_col("EpochSeconds", [1672597801, 1672597802, 1672597803])]
)

t_epoch = t_epoch_raw.update(
    [
        "NewTimestamp1 = epochSecondsToInstant(EpochSeconds)",
        "NewTimestamp2 = epochAutoToInstant(EpochSeconds)",
    ]
)

t_epoch_raw_meta = t_epoch_raw.meta_table
t_epoch_meta = t_epoch.meta_table
```

### Parse dates

When a date column holds strings in ISO 8601 format, such as `2023-01-01`, [`parseLocalDate`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseLocalDate(java.lang.String)) converts the column to a `LocalDate`:

```python test-set=1 order=t6,t6_raw,t6_meta,t6_raw_meta
from deephaven import new_table
from deephaven.column import string_col

t6_raw = new_table([string_col("Date", ["2023-01-01", "2023-01-02", "2023-01-03"])])

t6 = t6_raw.update("NewDate = parseLocalDate(Date)")

t6_raw_meta = t6_raw.meta_table
t6_meta = t6.meta_table
```

When the column uses a non-ISO format, describe that format to [`simple_date_format`](../time/datetime/simple_date_format.md), which returns a Java `SimpleDateFormat`. Parsing gives an [`Instant`](../query-language/types/date-time.md#instant), not a date, so the examples convert it back:

- The `SimpleDateFormat` parses a string with no time zone in the [engine time zone](#time-zones).
- [`timeZone`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#timeZone()) returns that time zone.
- [`toLocalDate`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#toLocalDate(java.time.Instant,java.time.ZoneId)), given the parsed `Instant` and the result of [`timeZone`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#timeZone()), returns the original date.

```python test-set=1 order=t7,t7_raw,t7_meta,t7_raw_meta
from deephaven import new_table
from deephaven.column import string_col
from deephaven.time import simple_date_format

t7_raw = new_table([string_col("Date", ["1/1/23", "1/2/23", "1/3/23"])])

input_format = simple_date_format("MM/dd/yy")

t7 = t7_raw.update(
    "NewDate = toLocalDate(input_format.parse(Date).toInstant(), timeZone())"
)

t7_raw_meta = t7_raw.meta_table
t7_meta = t7.meta_table
```

Here, the date is stored as an integer, so the example converts it to a `String` with `String.valueOf` before parsing it with a format from [`simple_date_format`](../time/datetime/simple_date_format.md):

```python test-set=1 order=t8,t8_raw,t8_meta,t8_raw_meta
from deephaven import new_table
from deephaven.column import int_col
from deephaven.time import simple_date_format

t8_raw = new_table([int_col("Date", [230101, 230102, 230103])])

input_format = simple_date_format("yyMMdd")

t8 = t8_raw.update(
    "NewDate = toLocalDate(input_format.parse(String.valueOf(Date)).toInstant(), timeZone())"
)

t8_raw_meta = t8_raw.meta_table
t8_meta = t8.meta_table
```

### Parse times

Much like dates, ISO-formatted time strings convert to `LocalTime` with [`parseLocalTime`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseLocalTime(java.lang.String)):

```python test-set=1 order=t9,t9_raw,t9_meta,t9_raw_meta
from deephaven import new_table
from deephaven.column import string_col

t9_raw = new_table([string_col("Time", ["12:30:00", "12:30:01", "12:30:02"])])

t9 = t9_raw.update("NewTime = parseLocalTime(Time)")

t9_raw_meta = t9_raw.meta_table
t9_meta = t9.meta_table
```

When the column uses a non-ISO format, describe that format to [`simple_date_format`](../time/datetime/simple_date_format.md). As with [dates](#parse-dates), parsing gives an [`Instant`](../query-language/types/date-time.md#instant), not a time, so the examples convert it back:

- The `SimpleDateFormat` parses a string with no time zone in the [engine time zone](#time-zones).
- [`timeZone`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#timeZone()) returns that time zone.
- [`toLocalTime`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#toLocalTime(java.time.Instant,java.time.ZoneId)), given the parsed `Instant` and the result of [`timeZone`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#timeZone()), returns the original time.

```python test-set=1 order=t10,t10_raw,t10_meta,t10_raw_meta
from deephaven import new_table
from deephaven.column import string_col
from deephaven.time import simple_date_format

t10_raw = new_table([string_col("Time", ["12.30.00", "12.30.01", "12.30.02"])])

input_format = simple_date_format("HH.mm.ss")

t10 = t10_raw.update(
    "NewTime = toLocalTime(input_format.parse(Time).toInstant(), timeZone())"
)

t10_raw_meta = t10_raw.meta_table
t10_meta = t10.meta_table
```

Times with no delimiters may be stored as integers. Convert them to strings first:

```python test-set=1 order=t11,t11_raw,t11_meta,t11_raw_meta
from deephaven import new_table
from deephaven.column import int_col
from deephaven.time import simple_date_format

t11_raw = new_table([int_col("Time", [123000, 123001, 123002])])

input_format = simple_date_format("HHmmss")

t11 = t11_raw.update(
    "NewTime = toLocalTime(input_format.parse(String.valueOf(Time)).toInstant(), timeZone())"
)

t11_raw_meta = t11_raw.meta_table
t11_meta = t11.meta_table
```

## Format date-time display

The [`format_columns`](../table-operations/format/format-columns.md) table operation changes how a date-time column is displayed. It does not change the underlying data. Reformat timestamps using [valid Java date-time formats](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/SimpleDateFormat.html). The `D` (day of year) pattern is not supported:

```python test-set=2 order=t1,t2,t3,t4,t5,t6
from deephaven import empty_table

t1 = empty_table(10).update("Timestamp = '2021-01-01T12:30:00Z' + i * 'P1d'")

# double digits for day and month, separated by spaces
t2 = t1.format_columns("Timestamp = Date(`dd MM y HH:mm:ss`)")

# separate year, month, and day by dashes
t3 = t1.format_columns("Timestamp = Date(`y-MM-dd HH:mm:ss`)")

# show the month as an abbreviated name
t4 = t1.format_columns("Timestamp = Date(`LLL d, y HH:mm:ss`)")

# show the day of the week and month as abbreviated names
t5 = t1.format_columns("Timestamp = Date(`EEE, LLL d, y HH:mm:ss`)")

# only day of month and time
t6 = t1.format_columns("Timestamp = Date(`dd HH:mm:ss`)")
```

[`format_columns`](../table-operations/format/format-columns.md) also works on ticking tables. This example formats a `Now` column, computed with [`now`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#now()), in a [`time_table`](../table-operations/create/timeTable.md) to show nanosecond precision:

```python ticking-table order=null
from deephaven import time_table

t_nanos = (
    time_table("PT1S")
    .update("Now = now()")
    .format_columns("Now = Date(`HH:mm:ss.SSSSSSSSS`)")
)
```

## Date-time settings in the UI

The **Settings** menu in the Deephaven IDE customizes how date-time values appear:

![The Deephaven IDE's Settings menu](../../assets/reference/cheat-sheets/ide-settings.png)

- **Time zone** sets the default time zone in which the UI displays date-time values in every table.
- **DateTime** sets the default format for date-time values.
- **Format by Column Name & Type** adds rules that set the display format for date-time columns with a given name in every table.

To change the format of one column in one table, use its column header menu. See [Set date-time format](../../how-to-guides/set-date-time-format.md) for details.

## Get current time information

### In a query

The [`now`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#now()), [`today`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#today()), and [`timeZone`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#timeZone()) functions return the current time, the current date as a `yyyy-MM-dd` string, and the [engine time zone](#time-zones). Use [`todayLocalDate`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#todayLocalDate()) to get the current date as a `LocalDate`:

```python test-set=3
from deephaven import empty_table

t = empty_table(1).update(
    ["CurrentTime = now()", "CurrentDate = today()", "CurrentTimeZone = timeZone()"]
)
```

### Outside of a query

In Python code, use [`dh_now`](../time/datetime/dh_now.md), [`dh_today`](../time/datetime/dh_today.md), and [`dh_time_zone`](../time/datetime/dh_time_zone.md):

```python test-set=3 order=:log
from deephaven.time import dh_now, dh_today, dh_time_zone

print("Current time:", dh_now())
print("Current date:", dh_today())
print("Current time zone:", dh_time_zone())
```

## Time zones

The engine time zone is the JVM's default time zone, which is set when the server starts. A `SimpleDateFormat` uses it to parse strings that have no time zone, and [`timeZone`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#timeZone()) returns it. It is not always the same as the time zone the UI displays, which the [Date-time settings in the UI](#date-time-settings-in-the-ui) section covers.

### Change engine time zone

To change the engine time zone, set the `user.timezone` JVM property at startup, as described in [Set the default time zone](../community-questions/set-timezone.md).

### Time zone aliases

Deephaven provides time zone aliases for some common time zones. The default aliases are `ET`, `CT`, `MT`, `PT`, `HK`, `JP`, and `LON`:

```python test-set=4
from deephaven import empty_table

zones2 = empty_table(1).update(["EasternTimeZone = 'ET'", "PacificTimeZone = 'PT'"])
```

[`time_zone_alias_add`](../time/datetime/time_zone_alias_add.md) adds a custom time zone alias, which queries can then use. [`time_zone_alias_rm`](../time/datetime/time_zone_alias_rm.md) removes it:

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
        # Parsing methods recognize time zone aliases
        "EasternTimeInstant = parseInstant(`2022-04-06T12:50:43 ET`)",
        "PacificTimeInstant = parseInstant(`2022-04-06T12:50:43 PT`)",
        # Custom aliases are also understood
        "SamoaTimeInstant = parseInstant(`2022-04-06T12:50:43 WST`)",
    ]
)

# Remove the custom time zone alias. Later queries that use "WST" fail
time_zone_alias_rm("WST")
```

## Manipulate timestamps

The examples in this section use the following table:

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

Date-time columns carry a lot of information. Deephaven's [auto-imported functions](../query-language/query-library/auto-imported/index.md) extract it:

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

The [`upperBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long)) and [`lowerBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)) functions group timestamps into bins at regular intervals. Each function accepts the bin interval in either of these forms:

- A number of nanoseconds, such as `5 * SECOND`, which uses a [built-in constant](#built-in-constants).
- A [duration](../query-language/types/durations.md), such as `'PT5S'`.

This example bins `Timestamp` into 10-minute intervals with a duration:

```python test-set=5 order=t_binned
t_binned = t.update(
    [
        "BinnedTimestamp = lowerBin(Timestamp, 'PT10m')",
    ]
)
```

[Downsampling](../../how-to-guides/downsampling.md) reduces a table to fewer rows, such as one row per bin. [`first_by`](../table-operations/group-and-aggregate/firstBy.md), [`last_by`](../table-operations/group-and-aggregate/lastBy.md), and [`avg_by`](../table-operations/group-and-aggregate/avgBy.md) downsample by grouping on the bin column:

```python test-set=5 order=t_binned_first,t_binned_last,t_binned_avg
# Downsample to first observation every 10 minutes
t_binned_first = t_binned.first_by("BinnedTimestamp")

# Downsample to last observation every 10 minutes
t_binned_last = t_binned.last_by("BinnedTimestamp")

# Downsample to average every 10 minutes
t_binned_avg = t_binned.select(["X", "BinnedTimestamp"]).avg_by("BinnedTimestamp")
```

To perform [multiple aggregations](../../how-to-guides/combined-aggregations.md) at once on binned timestamps, use [`agg_by`](../table-operations/group-and-aggregate/aggBy.md) with the [`agg`](/core/pydoc/code/deephaven.agg.html#module-deephaven.agg) suite. This example computes the open, high, low, and close of `X` in each 10-minute bin with [`agg.first`](../table-operations/group-and-aggregate/AggFirst.md), [`agg.max_`](../table-operations/group-and-aggregate/AggMax.md), [`agg.min_`](../table-operations/group-and-aggregate/AggMin.md), and [`agg.last`](../table-operations/group-and-aggregate/AggLast.md):

```python test-set=5
from deephaven import agg

t_ohlc = t_binned.agg_by(
    [agg.first("Open=X"), agg.max_("High=X"), agg.min_("Low=X"), agg.last("Close=X")],
    by="BinnedTimestamp",
)
```

### Time-based filtering

Deephaven's [filter methods](../../how-to-guides/use-filters.md) and [comparison operators](../../how-to-guides/operators.md#comparison-operators) work with timestamps. The last example combines a timestamp comparison with [`where_one_of`](../table-operations/filter/where-one-of.md), which keeps rows that match any of its filters:

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

[Arithmetic operators](../../how-to-guides/operators.md#arithmetic-operators) work on date-time values. Adding or subtracting a [duration](../query-language/types/durations.md) shifts an [`Instant`](../query-language/types/date-time.md#instant), and so does a [period](../query-language/types/periods.md) of days or weeks. A period with months or years needs a `ZonedDateTime`. Subtracting one [`Instant`](../query-language/types/date-time.md#instant) from another gives the difference in nanoseconds. Dividing by a [built-in constant](#built-in-constants) such as [`DAY`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#DAY) converts nanoseconds to another unit:

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

Date-time arithmetic also works directly in [filters](../../how-to-guides/use-filters.md), without new columns:

```python test-set=5
# Get data from less than 1000 days before 2024-04-03T15:30:00 UTC
t_recent = t.where("('2024-04-03T15:30:00.00 UTC' - Timestamp) / DAY < 1000")
```

### Built-in constants

Deephaven provides built-in constants, most of them for converting between time units. The [Math with date-times](#math-with-date-times) examples use [`YEAR_365`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#YEAR_365) and [`DAY`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#DAY). The following table lists the available constants:

| Constant                                                                                                    | Type        | Description                                            |
| ----------------------------------------------------------------------------------------------------------- | ----------- | ------------------------------------------------------ |
| [`DAY`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#DAY)                                             | `long`      | One day in nanoseconds.                                |
| [`DAYS_PER_NANO`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#DAYS_PER_NANO)                         | `double`    | Number of days per nanosecond.                         |
| [`HOUR`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#HOUR)                                           | `long`      | One hour in nanoseconds.                               |
| [`HOURS_PER_NANO`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#HOURS_PER_NANO)                       | `double`    | Number of hours per nanosecond.                        |
| [`MICRO`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#MICRO)                                         | `long`      | One microsecond in nanoseconds.                        |
| [`MILLI`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#MILLI)                                         | `long`      | One millisecond in nanoseconds.                        |
| [`MINUTE`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#MINUTE)                                       | `long`      | One minute in nanoseconds.                             |
| [`MINUTES_PER_NANO`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#MINUTES_PER_NANO)                   | `double`    | Number of minutes per nanosecond.                      |
| [`SECOND`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#SECOND)                                       | `long`      | One second in nanoseconds.                             |
| [`SECONDS_PER_NANO`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#SECONDS_PER_NANO)                   | `double`    | Number of seconds per nanosecond.                      |
| [`WEEK`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#WEEK)                                           | `long`      | One week in nanoseconds.                               |
| [`YEAR_365`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#YEAR_365)                                   | `long`      | One 365-day year in nanoseconds.                       |
| [`YEAR_AVG`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#YEAR_AVG)                                   | `long`      | One average (365.2425-day) year in nanoseconds.        |
| [`YEARS_PER_NANO_365`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#YEARS_PER_NANO_365)               | `double`    | Number of 365-day years per nanosecond.                |
| [`YEARS_PER_NANO_AVG`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#YEARS_PER_NANO_AVG)               | `double`    | Number of average (365.2425-day) years per nanosecond. |
| [`ZERO_LENGTH_INSTANT_ARRAY`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#ZERO_LENGTH_INSTANT_ARRAY) | `Instant[]` | A zero-length array of instants.                       |

These constants are available in query strings. In Python code, use `datetime.timedelta` or `numpy` types instead.

## Time-based joins

Timestamps from different datasets rarely match exactly, so [exact-match joins](../../how-to-guides/joins-exact-relational.md) on timestamps find few matches. The [`aj`](../table-operations/join/aj.md) and [`raj`](../table-operations/join/raj.md) joins match on the closest timestamp instead. See [Inexact, time-series, and range joins](../../how-to-guides/joins-timeseries-range.md) for more.

The examples in this section join the table `t` from [Manipulate timestamps](#manipulate-timestamps), which has one row per minute and a random `X` column, to a table whose timestamps are offset by random amounts.

### `aj` (as-of join)

The as-of join [`aj`](../table-operations/join/aj.md) joins tables on timestamp columns by choosing values from the right key column that are as close as possible to values in the left key column _without going over_:

```python test-set=5 order=as_of_joined,t_noisy
t_noisy = empty_table(2 * 365 * 24 * 60).update(
    [
        "Timestamp = '2020-01-01T00:00:00.00 UTC' + 'PT1m'.multipliedBy(ii) + 'PT0.1s'.multipliedBy(randomInt(0, 10*60-1))",
        "Y = randomGaussian(0, 1)",
    ]
)

as_of_joined = t.aj(t_noisy, on="Timestamp", joins="Y")
```

[`aj`](../table-operations/join/aj.md) can also match one or more key columns exactly before the as-of match. List the timestamp column last in `on`. This example matches `Group` exactly:

```python test-set=5 order=as_of_joined_exact,t_group,t_noisy_group
t_group = t.update("Group = randomInt(0, 5)")
t_noisy_group = t_noisy.update("Group = randomInt(0, 5)")

as_of_joined_exact = t_group.aj(t_noisy_group, on=["Group", "Timestamp"], joins="Y")
```

### `raj` (reverse as-of join)

The reverse as-of join [`raj`](../table-operations/join/raj.md) joins tables on timestamp columns by choosing values from the right key column that are as close as possible to values in the left key column _without going under_. This example uses `t_noisy` from the [`aj` (as-of join)](#aj-as-of-join) section:

```python test-set=5 order=reverse_as_of_joined
reverse_as_of_joined = t.raj(t_noisy, on="Timestamp", joins="Y")
```

Like [`aj`](../table-operations/join/aj.md), [`raj`](../table-operations/join/raj.md) can match key columns exactly before the timestamp column. This example reuses `t_group` and `t_noisy_group` from the [`aj` (as-of join)](#aj-as-of-join) section:

```python test-set=5 order=reverse_as_of_joined_exact
reverse_as_of_joined_exact = t_group.raj(
    t_noisy_group, on=["Group", "Timestamp"], joins="Y"
)
```

## Work with Python time types

Python code commonly uses date-time types from the standard [`datetime`](https://docs.python.org/3/library/datetime.html) module and the [`numpy`](https://numpy.org/doc/stable/reference/arrays.datetime.html) and [`pandas`](https://pandas.pydata.org/docs/user_guide/timeseries.html) libraries. Deephaven provides interoperability with these types through the [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) module.

### Python to Java

The [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) module provides the following functions for converting `datetime`, `numpy`, and `pandas` types to Java types for use in query strings:

- [`to_j_time_zone`](../time/datetime/to_j_time_zone.md)
- [`to_j_local_date`](../time/datetime/to_j_local_date.md)
- [`to_j_local_time`](../time/datetime/to_j_local_time.md)
- [`to_j_instant`](../time/datetime/to_j_instant.md)
- [`to_j_zdt`](../time/datetime/to_j_zdt.md)
- [`to_j_duration`](../time/datetime/to_j_duration.md)
- [`to_j_period`](../time/datetime/to_j_period.md)

> [!WARNING]
> For performance, do not call these functions inside query strings. Call them outside the query string to create objects, then use those objects in the query string.

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

These functions each accept several argument types, such as strings and the matching `datetime`, `numpy`, or `pandas` types. The [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) Pydoc lists the types each function accepts.

### Java to Python

The [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) module also provides the following functions for converting Java types to their Python equivalents:

- [`to_date`](../time/datetime/to_date.md)
- [`to_time`](../time/datetime/to_time.md)
- [`to_datetime`](../time/datetime/to_datetime.md)
- [`to_timedelta`](../time/datetime/to_timedelta.md)
- [`to_np_datetime64`](../time/datetime/to_np_datetime64.md)
- [`to_np_timedelta64`](../time/datetime/to_np_timedelta64.md)
- [`to_pd_timestamp`](../time/datetime/to_pd_timestamp.md)
- [`to_pd_timedelta`](../time/datetime/to_pd_timedelta.md)

A common use is inside a Python function that a query string calls with date-time arguments:

```python test-set=7
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
> For this example, the built-in [`diffSeconds`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#diffSeconds(java.time.Instant,java.time.Instant)) function is the better choice. Converting to Python types is useful when the query language has no function for the task.

## Related documentation

- [Work with time](../../conceptual/time-in-deephaven.md)
- [Parallelizing queries](../../conceptual/query-engine/parallelization.md)
- [Date-time literals in query strings](../../how-to-guides/date-time-literals.md)
- [Create a time table](../../how-to-guides/time-table.md)
- [Formulas](../../how-to-guides/formulas.md)
- [Filter table data](../../how-to-guides/use-filters.md)
- [Operators](../../how-to-guides/operators.md)
- [Downsampling](../../how-to-guides/downsampling.md)
- [Combined aggregations](../../how-to-guides/combined-aggregations.md)
- [Exact and relational joins](../../how-to-guides/joins-exact-relational.md)
- [Inexact, time-series, and range joins](../../how-to-guides/joins-timeseries-range.md)
- [Set date-time format](../../how-to-guides/set-date-time-format.md)
- [Set the default time zone](../community-questions/set-timezone.md)
- [Date-time types](../query-language/types/date-time.md)
- [Durations](../query-language/types/durations.md)
- [Periods](../query-language/types/periods.md)
- [Auto-imported functions](../query-language/query-library/auto-imported/index.md)
- [`agg.first`](../table-operations/group-and-aggregate/AggFirst.md)
- [`agg.last`](../table-operations/group-and-aggregate/AggLast.md)
- [`agg.max_`](../table-operations/group-and-aggregate/AggMax.md)
- [`agg.min_`](../table-operations/group-and-aggregate/AggMin.md)
- [`agg_by`](../table-operations/group-and-aggregate/aggBy.md)
- [`aj`](../table-operations/join/aj.md)
- [`avg_by`](../table-operations/group-and-aggregate/avgBy.md)
- [`datetime_col`](../table-operations/create/dateTimeCol.md)
- [`dh_now`](../time/datetime/dh_now.md)
- [`dh_time_zone`](../time/datetime/dh_time_zone.md)
- [`dh_today`](../time/datetime/dh_today.md)
- [`empty_table`](../table-operations/create/emptyTable.md)
- [`first_by`](../table-operations/group-and-aggregate/firstBy.md)
- [`format_columns`](../table-operations/format/format-columns.md)
- [`int_col`](../table-operations/create/intCol.md)
- [`last_by`](../table-operations/group-and-aggregate/lastBy.md)
- [`long_col`](../table-operations/create/longCol.md)
- [`meta_table`](../table-operations/metadata/meta_table.md)
- [`new_table`](../table-operations/create/newTable.md)
- [`raj`](../table-operations/join/raj.md)
- [`select`](../table-operations/select/select.md)
- [`simple_date_format`](../time/datetime/simple_date_format.md)
- [`string_col`](../table-operations/create/stringCol.md)
- [`time_table`](../table-operations/create/timeTable.md)
- [`time_zone_alias_add`](../time/datetime/time_zone_alias_add.md)
- [`time_zone_alias_rm`](../time/datetime/time_zone_alias_rm.md)
- [`to_date`](../time/datetime/to_date.md)
- [`to_datetime`](../time/datetime/to_datetime.md)
- [`to_j_duration`](../time/datetime/to_j_duration.md)
- [`to_j_instant`](../time/datetime/to_j_instant.md)
- [`to_j_local_date`](../time/datetime/to_j_local_date.md)
- [`to_j_local_time`](../time/datetime/to_j_local_time.md)
- [`to_j_period`](../time/datetime/to_j_period.md)
- [`to_j_time_zone`](../time/datetime/to_j_time_zone.md)
- [`to_j_zdt`](../time/datetime/to_j_zdt.md)
- [`to_np_datetime64`](../time/datetime/to_np_datetime64.md)
- [`to_np_timedelta64`](../time/datetime/to_np_timedelta64.md)
- [`to_pd_timedelta`](../time/datetime/to_pd_timedelta.md)
- [`to_pd_timestamp`](../time/datetime/to_pd_timestamp.md)
- [`to_time`](../time/datetime/to_time.md)
- [`to_timedelta`](../time/datetime/to_timedelta.md)
- [`update`](../table-operations/select/update.md)
- [`where`](../table-operations/filter/where.md)
- [`where_one_of`](../table-operations/filter/where-one-of.md)
- [`deephaven.time` Pydoc](/core/pydoc/code/deephaven.time.html#module-deephaven.time)
- [`deephaven.agg` Pydoc](/core/pydoc/code/deephaven.agg.html#module-deephaven.agg)
- [`DateTimeUtils` Javadoc](/core/javadoc/io/deephaven/time/DateTimeUtils.html)
