---
title: Time and Calendars
---

This chapter covers working with time and calendars in Deephaven queries. Since Deephaven specializes in real-time operations, understanding how to work with time is crucial to building effective analyses.

It covers the following topics:

- [Time data types](#time-data-types)
- [Time constants](#time-constants)
- [Create temporal data](#create-temporal-data)
- [Time arithmetic and filtering](#time-arithmetic-and-filtering)
- [Time zones](#time-zones)
- [Business calendars](#business-calendars)
- [Aggregate by time](#aggregate-by-time)
- [Put it all together](#put-it-all-together)

## Time data types

Deephaven natively supports the following [`java.time`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/package-summary.html) types:

- [**`Instant`**](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html): A specific point in time.
- [**`ZonedDateTime`**](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZonedDateTime.html): A specific point in time with time zone information.
- [**`LocalDate`**](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html): A date without time zone information.
- [**`LocalTime`**](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalTime.html): A time of day without date or time zone information.
- [**`Duration`**](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html): A length of time, such as 5 minutes or 1 hour.
- [**`Period`**](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html): A date-based amount of time, such as 2 months.
- [**`ZoneId`**](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZoneId.html): A time zone, such as `America/New_York`.

This chapter works mostly with [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html), plus [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZonedDateTime.html), [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html), and [`Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html). See [Work with time](../../conceptual/time-in-deephaven.md#natively-supported-date-time-types) for details on all of them.

## Time constants

Deephaven offers built-in constants for common time periods, defined in [`DateTimeUtils`](/core/javadoc/io/deephaven/time/DateTimeUtils.html). Each constant is a number of nanoseconds:

```python test-set=1
from deephaven import empty_table

# Several of the constants available
time_constants = empty_table(1).update(
    [
        "Millisecond = MILLI",
        "Second = SECOND",
        "Minute = MINUTE",
        "Hour = HOUR",
        "Day = DAY",
        "Week = WEEK",
        "Year = YEAR_365",
    ]
)
```

## Create temporal data

You can create temporal data with built-in [query string](./query-strings.md) methods. For instance, [`now`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#now()) returns the current time as an [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html), and adding a time constant to an `Instant` gives a sequence of later `Instant`s:

```python test-set=2
from deephaven import empty_table

temporal_data_table = empty_table(10).update(
    [
        "CurrentTime = now()",
        "SequentialSeconds = now() + ii * SECOND",
        "SequentialMinutes = now() + ii * MINUTE",
    ]
)
```

## Time arithmetic and filtering

Time arithmetic in tables uses built-in methods, either called directly or through the `+` and `-` operators. See [`DateTimeUtils`](/core/javadoc/io/deephaven/time/DateTimeUtils.html) for the available constants and methods.

In a query string, a date-time value goes in single quotes and ends with its time zone, such as `'2025-06-01T09:30Z'` or `'2025-06-01T09:30 ET'`. The `Z` suffix means UTC, and `ET` means US Eastern Time; [Time zones](#time-zones) covers time zone names. A duration also goes in single quotes, such as `'PT1H'` for one hour. A string goes in backticks, such as `` `A` ``. [Query strings](./query-strings.md#literals) covers literals in full.

```python test-set=3
from deephaven import empty_table

time_arithmetic_table = empty_table(10).update(
    [
        "Timestamp = '2025-06-01T09:30Z' + ii * MINUTE",
        "EpochMillis = epochMillis(Timestamp)",
        "TimestampMinusOneHour = minus(Timestamp, 'PT1H')",
        "TimestampPlusThirtyMinutes = plus(Timestamp, 'PT30M')",
        "UpperBin15Minutes = upperBin(Timestamp, 'PT15M')",
        "LowerBin8Minutes = lowerBin(Timestamp, 'PT8M')",
    ]
)
```

[`epochMillis`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochMillis(java.time.Instant)) converts a timestamp to milliseconds since 1970-01-01 UTC. [`upperBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long)) and [`lowerBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)) round a timestamp up or down to a multiple of an interval. The [Aggregations over time buckets](#aggregations-over-time-buckets) section uses `lowerBin` to group rows into 15-minute buckets.

[`plus`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#plus(java.time.Instant,long)), [`minus`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#minus(java.time.Instant,long)), [`upperBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long)), and [`lowerBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)) all accept a length of time in either of these forms:

- A time constant expression, such as `15 * MINUTE`, which is a count of nanoseconds.
- An ISO 8601 duration string, such as `'PT15M'`, which is a [`Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html).

[`plus`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#plus(java.time.Instant,java.time.Period)) and [`minus`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#minus(java.time.Instant,java.time.Period)) also accept a date-based [`Period`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html), such as `'P1D'`.

Filtering temporal data with [`where`](../../reference/table-operations/filter/where.md) works the same way as filtering numeric data:

```python test-set=3 order=times_greater_than,times_less_than,times_in_range
times_greater_than = time_arithmetic_table.where("Timestamp >= '2025-06-01T09:34Z'")
times_less_than = time_arithmetic_table.where("Timestamp < '2025-06-01T09:37Z'")
times_in_range = time_arithmetic_table.where(
    ["Timestamp > '2025-06-01T09:34Z'", "Timestamp <= '2025-06-01T09:37Z'"]
)
```

## Time zones

The same instant can have a different local date and time in different time zones. For example, if it's 6 PM in Los Angeles during daylight saving time, it's 9 AM the next day in Shanghai. Methods that extract a date or time component, such as [`secondOfDay`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#secondOfDay(java.time.Instant,java.time.ZoneId,boolean)), [`dayOfYear`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#dayOfYear(java.time.Instant,java.time.ZoneId)), and [`hourOfDay`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#hourOfDay(java.time.Instant,java.time.ZoneId,boolean)), take a time zone because their result depends on it.

Pass a time zone in single quotes, in either of these forms:

- A full name, such as `'Asia/Shanghai'`.
- A short alias, such as `'PT'` for Pacific Time (`America/Los_Angeles`) or `'ET'` for Eastern Time (`America/New_York`).

The alias `'PT'` is unrelated to the `PT` prefix of duration strings such as `'PT1H'`.

```python test-set=4
from deephaven import empty_table

time_zone_arithmetic_table = empty_table(10).update(
    [
        "Timestamp = '2025-06-01T09:30Z' + 2 * ii * HOUR",
        "SecondOfDayLosAngeles = secondOfDay(Timestamp, 'PT', true)",
        "DayOfYearLosAngeles = dayOfYear(Timestamp, 'PT')",
        "HourOfDayLosAngeles = hourOfDay(Timestamp, 'PT', true)",
        "SecondOfDayShanghai = secondOfDay(Timestamp, 'Asia/Shanghai', true)",
        "DayOfYearShanghai = dayOfYear(Timestamp, 'Asia/Shanghai')",
        "HourOfDayShanghai = hourOfDay(Timestamp, 'Asia/Shanghai', true)",
    ]
)
```

The final `true` argument to [`secondOfDay`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#secondOfDay(java.time.Instant,java.time.ZoneId,boolean)) and [`hourOfDay`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#hourOfDay(java.time.Instant,java.time.ZoneId,boolean)) sets `asLocalTime`. With `asLocalTime` set to `true`, they measure from local midnight by the wall clock, so a 9:30 AM local timestamp gives the same result on every day, including days when daylight saving time starts or ends.

The previous operations use [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) values. An `Instant` contains no time zone information, so each method call needs a time zone argument such as `'PT'` or `'Asia/Shanghai'`. You can store the time zone in the data instead by converting to a [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZonedDateTime.html) with [`toZonedDateTime`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#toZonedDateTime(java.time.Instant,java.time.ZoneId)):

```python test-set=5
from deephaven import empty_table

zoned_time_table = (
    empty_table(10)
    .update(
        [
            "Timestamp = '2025-06-01T09:30Z' + 2 * ii * HOUR",
            "TimestampLosAngeles = toZonedDateTime(Timestamp, 'PT')",
            "TimestampShanghai = toZonedDateTime(Timestamp, 'Asia/Shanghai')",
            "SecondOfDayLosAngeles = secondOfDay(TimestampLosAngeles, true)",
            "DayOfYearLosAngeles = dayOfYear(TimestampLosAngeles)",
            "HourOfDayLosAngeles = hourOfDay(TimestampLosAngeles, true)",
            "SecondOfDayShanghai = secondOfDay(TimestampShanghai, true)",
            "DayOfYearShanghai = dayOfYear(TimestampShanghai)",
            "HourOfDayShanghai = hourOfDay(TimestampShanghai, true)",
        ]
    )
    .drop_columns("Timestamp")
)
```

## Business calendars

[Business calendars](../../how-to-guides/business-calendar.md) help you work with trading days, business hours, and holidays.

### Built-in calendars

Deephaven includes three built-in calendars: `UTC` and the example calendars `USNYSE_EXAMPLE` and `USBANK_EXAMPLE`. [`calendar_names`](../../reference/time/calendar/calendar-names.md) lists the available calendars, and [`calendar`](../../reference/time/calendar/calendar.md) gets one by name:

```python test-set=6 order=:log
from deephaven.calendar import calendar_names, calendar

print(calendar_names())
nyse_cal = calendar("USNYSE_EXAMPLE")
```

> [!NOTE]
> The `USNYSE_EXAMPLE` and `USBANK_EXAMPLE` calendars are examples. For any production system, use a custom calendar. See [Work with calendars](../../how-to-guides/business-calendar.md#create-a-calendar) to create one.

### Filter and count with a calendar

You can use a calendar to:

- [Filter](../../reference/table-operations/filter/where.md) data to business days with [`isBusinessDay`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html#isBusinessDay(java.time.Instant)).
- [Filter](../../reference/table-operations/filter/where.md) data to business hours with [`isBusinessTime`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html#isBusinessTime(java.time.Instant)).
- Count business days with [`numberBusinessDates`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html#numberBusinessDates(java.time.Instant,java.time.Instant)).

The following examples use a table of synthetic trades. It has one trade every 30 seconds around the clock for five days, starting on Monday, July 14, 2025. The data therefore includes nights and a Saturday morning:

```python test-set=6
from deephaven import empty_table

# 5 days of trading data, one trade every 30 seconds
num_trades = 5 * 24 * 60 * 2

trading_data = empty_table(num_trades).update(
    [
        "Timestamp = '2025-07-14T09:30 ET' + ii * 30 * SECOND",
        "Ticker = (ii % 2 == 0) ? `A` : `B`",
        "Price = (ii % 2 == 0) ? randomDouble(50, 75) : randomDouble(120, 160)",
        "Size = randomDouble(0, 10)",
    ]
)
```

A query string can use a Python variable by name and call its methods, so `nyse_cal.isBusinessDay(Timestamp)` calls the calendar's [`isBusinessDay`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html#isBusinessDay(java.time.Instant)) method for each row. [Query strings](./query-strings.md#python-in-query-strings) covers this in full.

The following example [filters](../../reference/table-operations/filter/where.md) the trades to business days and to business hours, then counts business days. [`numberBusinessDates`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html#numberBusinessDates(java.time.Instant,java.time.Instant)) counts the business dates between two times, including both ends. `BizDaysFromStart` is 1 for Monday's trades and rises to 5 for Friday's trades. Saturday isn't a business day, so Saturday's trades also get 5:

```python test-set=6 order=business_days_only,business_hours_only,business_day_counts
business_days_only = trading_data.where("nyse_cal.isBusinessDay(Timestamp)")
business_hours_only = trading_data.where("nyse_cal.isBusinessTime(Timestamp)")

business_day_counts = trading_data.update(
    [
        "BizDaysFromStart = nyse_cal.numberBusinessDates('2025-07-14T00:00:00 ET', Timestamp)"
    ]
)
```

## Aggregate by time

Grouping rows by date or by fixed intervals of time, then [aggregating](../../how-to-guides/combined-aggregations.md) each group, is the core of most time-series analysis.

### Daily summaries

To summarize data by day, convert each timestamp to a date with [`toLocalDate`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#toLocalDate(java.time.Instant,java.time.ZoneId)), then aggregate with [`agg_by`](../../reference/table-operations/group-and-aggregate/aggBy.md). For example, the following query uses the `trading_data` table from [Filter and count with a calendar](#filter-and-count-with-a-calendar) to calculate the number of trades, the total size, and the average price for each ticker on each day:

```python test-set=6 order=daily_summary
from deephaven import agg

daily_summary = trading_data.update(
    ["Date = toLocalDate(Timestamp, 'ET')"]
).agg_by(
    [
        agg.count_("NumTrades"),
        agg.sum_("TotalSize = Size"),
        agg.avg("AvgPrice = Price"),
    ],
    by=["Date", "Ticker"],
)
```

### Aggregations over time buckets

Time buckets group data into fixed intervals so you can analyze trends across them. The following query uses [`lowerBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)) to place each trade into a 15-minute bucket, then calculates the average trade price and size for each ticker in each bucket:

```python test-set=6 order=avg_by_time_bucket
avg_by_time_bucket = trading_data.update(
    ["TimeBucket = lowerBin(Timestamp, 15 * MINUTE)"]
).agg_by(
    [agg.avg("AvgPrice = Price"), agg.avg("AvgSize = Size")],
    by=["TimeBucket", "Ticker"],
)
```

## Put it all together

The following example combines the business-hours [filter](../../reference/table-operations/filter/where.md) from [Filter and count with a calendar](#filter-and-count-with-a-calendar) with the daily summary from [Daily summaries](#daily-summaries), a combination common in financial data analysis. It reuses the `nyse_cal` calendar and differs from those examples in two ways:

- Its data starts on January 1, an NYSE holiday, so the calendar [filter](../../reference/table-operations/filter/where.md) removes that whole day.
- Its query uses [`minuteOfDay`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#minuteOfDay(java.time.Instant,java.time.ZoneId,boolean)) to get each trade's minute of the day, then flags trades in the first hour after the open.

```python test-set=6 order=market_data,processed_data,market_daily_summary
# Create synthetic market data: a row every 5 minutes, rotating among three tickers
market_data = empty_table(1000).update(
    [
        "Timestamp = '2024-01-01T09:00:00 ET' + ii * 5 * MINUTE",
        "Ticker = ii % 3 == 0 ? `AAPL` : (ii % 3 == 1 ? `GOOGL` : `MSFT`)",
        "Price = 150 + randomDouble(-50, 50)",
        "Size = randomInt(100, 10000)",
    ]
)

# Filter to business hours and add time-based features
# January 1 is an NYSE holiday, so the filter removes that whole day
processed_data = market_data.where(["nyse_cal.isBusinessTime(Timestamp)"]).update(
    [
        "Date = toLocalDate(Timestamp, 'ET')",
        "MinuteOfDay = minuteOfDay(Timestamp, 'ET', true)",
        # Before 10:30 AM (630 minutes after midnight): the first hour after the 9:30 AM open
        "IsEarlyTrading = MinuteOfDay < 630",
    ]
)

# Calculate daily summary statistics
market_daily_summary = processed_data.agg_by(
    [
        agg.count_("NumTrades"),
        agg.sum_(["TotalSize = Size", "EarlyTrades = IsEarlyTrading"]),
        agg.avg("AvgPrice = Price"),
    ],
    by=["Date", "Ticker"],
)
```

## Next steps

To go further with time-series analysis, see:

- [Built-in time functions](../../reference/query-language/query-library/auto-imported/time.md)
- [Rolling window calculations](../../how-to-guides/rolling-aggregations.md)
- [Time-based joins](../../how-to-guides/joins-timeseries-range.md)
- [Work with time](../../conceptual/time-in-deephaven.md)
- [Work with calendars](../../how-to-guides/business-calendar.md)
