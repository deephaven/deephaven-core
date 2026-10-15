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

```groovy test-set=1
// Several of the constants available
timeConstants = emptyTable(1).update(
        "Millisecond = MILLI",
        "Second = SECOND",
        "Minute = MINUTE",
        "Hour = HOUR",
        "Day = DAY",
        "Week = WEEK",
        "Year = YEAR_365"
)
```

## Create temporal data

You can create temporal data with built-in [query string](./query-strings.md) methods. For instance, [`now`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#now()) returns the current time as an [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html), and adding a time constant to an `Instant` gives a sequence of later `Instant`s:

```groovy test-set=2
temporalDataTable = emptyTable(10).update(
        "CurrentTime = now()",
        "SequentialSeconds = now() + ii * SECOND",
        "SequentialMinutes = now() + ii * MINUTE"
)
```

## Time arithmetic and filtering

Time arithmetic in tables uses built-in methods, either called directly or through the `+` and `-` operators. See [`DateTimeUtils`](/core/javadoc/io/deephaven/time/DateTimeUtils.html) for the available constants and methods.

In a query string, a date-time value goes in single quotes and ends with its time zone, such as `'2025-06-01T09:30Z'` or `'2025-06-01T09:30 ET'`. The `Z` suffix means UTC, and `ET` means US Eastern Time; [Time zones](#time-zones) covers time zone names. A duration also goes in single quotes, such as `'PT1H'` for one hour. A string goes in backticks, such as `` `A` ``. [Query strings](./query-strings.md#literals) covers literals in full.

```groovy test-set=3
timeArithmeticTable = emptyTable(10).update(
        "Timestamp = '2025-06-01T09:30Z' + ii * MINUTE",
        "EpochMillis = epochMillis(Timestamp)",
        "TimestampMinusOneHour = minus(Timestamp, 'PT1H')",
        "TimestampPlusThirtyMinutes = plus(Timestamp, 'PT30M')",
        "UpperBin15Minutes = upperBin(Timestamp, 'PT15M')",
        "LowerBin8Minutes = lowerBin(Timestamp, 'PT8M')"
)
```

[`epochMillis`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochMillis(java.time.Instant)) converts a timestamp to milliseconds since 1970-01-01 UTC. [`upperBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long)) and [`lowerBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)) round a timestamp up or down to a multiple of an interval. The [Aggregations over time buckets](#aggregations-over-time-buckets) section uses `lowerBin` to group rows into 15-minute buckets.

[`plus`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#plus(java.time.Instant,long)), [`minus`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#minus(java.time.Instant,long)), [`upperBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long)), and [`lowerBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)) all accept a length of time in either of these forms:

- A time constant expression, such as `15 * MINUTE`, which is a count of nanoseconds.
- An ISO 8601 duration string, such as `'PT15M'`, which is a [`Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html).

[`plus`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#plus(java.time.Instant,java.time.Period)) and [`minus`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#minus(java.time.Instant,java.time.Period)) also accept a date-based [`Period`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html), such as `'P1D'`.

Filtering temporal data with [`where`](../../reference/table-operations/filter/where.md) works the same way as filtering numeric data:

```groovy test-set=3 order=timesGreaterThan,timesLessThan,timesInRange
timesGreaterThan = timeArithmeticTable.where("Timestamp >= '2025-06-01T09:34Z'")
timesLessThan = timeArithmeticTable.where("Timestamp < '2025-06-01T09:37Z'")
timesInRange = timeArithmeticTable.where(
        "Timestamp > '2025-06-01T09:34Z'",
        "Timestamp <= '2025-06-01T09:37Z'"
)
```

## Time zones

The same instant can have a different local date and time in different time zones. For example, if it's 6 PM in Los Angeles during daylight saving time, it's 9 AM the next day in Shanghai. Methods that extract a date or time component, such as [`secondOfDay`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#secondOfDay(java.time.Instant,java.time.ZoneId,boolean)), [`dayOfYear`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#dayOfYear(java.time.Instant,java.time.ZoneId)), and [`hourOfDay`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#hourOfDay(java.time.Instant,java.time.ZoneId,boolean)), take a time zone because their result depends on it.

Pass a time zone in single quotes, in either of these forms:

- A full name, such as `'Asia/Shanghai'`.
- A short alias, such as `'PT'` for Pacific Time (`America/Los_Angeles`) or `'ET'` for Eastern Time (`America/New_York`).

The alias `'PT'` is unrelated to the `PT` prefix of duration strings such as `'PT1H'`.

```groovy test-set=4
timeZoneArithmeticTable = emptyTable(10).update(
        "Timestamp = '2025-06-01T09:30Z' + 2 * ii * HOUR",
        "SecondOfDayLosAngeles = secondOfDay(Timestamp, 'PT', true)",
        "DayOfYearLosAngeles = dayOfYear(Timestamp, 'PT')",
        "HourOfDayLosAngeles = hourOfDay(Timestamp, 'PT', true)",
        "SecondOfDayShanghai = secondOfDay(Timestamp, 'Asia/Shanghai', true)",
        "DayOfYearShanghai = dayOfYear(Timestamp, 'Asia/Shanghai')",
        "HourOfDayShanghai = hourOfDay(Timestamp, 'Asia/Shanghai', true)"
)
```

The final `true` argument to [`secondOfDay`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#secondOfDay(java.time.Instant,java.time.ZoneId,boolean)) and [`hourOfDay`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#hourOfDay(java.time.Instant,java.time.ZoneId,boolean)) sets `asLocalTime`. With `asLocalTime` set to `true`, they measure from local midnight by the wall clock, so a 9:30 AM local timestamp gives the same result on every day, including days when daylight saving time starts or ends.

The previous operations use [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) values. An `Instant` contains no time zone information, so each method call needs a time zone argument such as `'PT'` or `'Asia/Shanghai'`. You can store the time zone in the data instead by converting to a [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZonedDateTime.html) with [`toZonedDateTime`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#toZonedDateTime(java.time.Instant,java.time.ZoneId)):

```groovy test-set=5
zonedTimeTable = emptyTable(10)
        .update(
                "Timestamp = '2025-06-01T09:30Z' + 2 * ii * HOUR",
                "TimestampLosAngeles = toZonedDateTime(Timestamp, 'PT')",
                "TimestampShanghai = toZonedDateTime(Timestamp, 'Asia/Shanghai')",
                "SecondOfDayLosAngeles = secondOfDay(TimestampLosAngeles, true)",
                "DayOfYearLosAngeles = dayOfYear(TimestampLosAngeles)",
                "HourOfDayLosAngeles = hourOfDay(TimestampLosAngeles, true)",
                "SecondOfDayShanghai = secondOfDay(TimestampShanghai, true)",
                "DayOfYearShanghai = dayOfYear(TimestampShanghai)",
                "HourOfDayShanghai = hourOfDay(TimestampShanghai, true)"
        )
        .dropColumns("Timestamp")
```

## Business calendars

[Business calendars](../../how-to-guides/business-calendar.md) help you work with trading days, business hours, and holidays.

### Built-in calendars

Deephaven includes three built-in calendars: `UTC` and the example calendars `USNYSE_EXAMPLE` and `USBANK_EXAMPLE`. [`calendarNames`](/core/javadoc/io/deephaven/time/calendar/Calendars.html#calendarNames()) lists the available calendars, and [`calendar`](/core/javadoc/io/deephaven/time/calendar/Calendars.html#calendar(java.lang.String)) gets one by name:

```groovy test-set=6 order=:log
println calendarNames()
nyseCal = calendar("USNYSE_EXAMPLE")
```

> [!NOTE]
> The `USNYSE_EXAMPLE` and `USBANK_EXAMPLE` calendars are examples. For any production system, use a custom calendar. See [Work with calendars](../../how-to-guides/business-calendar.md#create-a-calendar) to create one.

### Filter and count with a calendar

You can use a calendar to:

- [Filter](../../reference/table-operations/filter/where.md) data to business days with [`isBusinessDay`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html#isBusinessDay(java.time.Instant)).
- [Filter](../../reference/table-operations/filter/where.md) data to business hours with [`isBusinessTime`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html#isBusinessTime(java.time.Instant)).
- Count business days with [`numberBusinessDates`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html#numberBusinessDates(java.time.Instant,java.time.Instant)).

The following examples use a table of synthetic trades. It has one trade every 30 seconds around the clock for five days, starting on Monday, July 14, 2025. The data therefore includes nights and a Saturday morning:

```groovy test-set=6
// 5 days of trading data, one trade every 30 seconds
numTrades = 5 * 24 * 60 * 2

tradingData = emptyTable(numTrades).update(
        "Timestamp = '2025-07-14T09:30 ET' + ii * 30 * SECOND",
        "Ticker = (ii % 2 == 0) ? `A` : `B`",
        "Price = (ii % 2 == 0) ? randomDouble(50, 75) : randomDouble(120, 160)",
        "Size = randomDouble(0, 10)"
)
```

A query string can use a Groovy variable by name and call its methods, so `nyseCal.isBusinessDay(Timestamp)` calls the calendar's [`isBusinessDay`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html#isBusinessDay(java.time.Instant)) method for each row. [Query strings](./query-strings.md#groovy-in-query-strings) covers this in full.

The following example [filters](../../reference/table-operations/filter/where.md) the trades to business days and to business hours, then counts business days. [`numberBusinessDates`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html#numberBusinessDates(java.time.Instant,java.time.Instant)) counts the business dates between two times, including both ends. `BizDaysFromStart` is 1 for Monday's trades and rises to 5 for Friday's trades. Saturday isn't a business day, so Saturday's trades also get 5:

```groovy test-set=6 order=businessDaysOnly,businessHoursOnly,businessDayCounts
businessDaysOnly = tradingData.where("nyseCal.isBusinessDay(Timestamp)")
businessHoursOnly = tradingData.where("nyseCal.isBusinessTime(Timestamp)")

businessDayCounts = tradingData.update(
        "BizDaysFromStart = nyseCal.numberBusinessDates('2025-07-14T00:00:00 ET', Timestamp)"
)
```

## Aggregate by time

Grouping rows by date or by fixed intervals of time, then [aggregating](../../how-to-guides/combined-aggregations.md) each group, is the core of most time-series analysis.

### Daily summaries

To summarize data by day, convert each timestamp to a date with [`toLocalDate`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#toLocalDate(java.time.Instant,java.time.ZoneId)), then aggregate with [`aggBy`](../../reference/table-operations/group-and-aggregate/aggBy.md). For example, the following query uses the `tradingData` table from [Filter and count with a calendar](#filter-and-count-with-a-calendar) to calculate the number of trades, the total size, and the average price for each ticker on each day:

```groovy test-set=6 order=dailySummary
dailySummary = tradingData
        .update("Date = toLocalDate(Timestamp, 'ET')")
        .aggBy(
                [AggCount("NumTrades"), AggSum("TotalSize = Size"), AggAvg("AvgPrice = Price")],
                "Date", "Ticker"
        )
```

### Aggregations over time buckets

Time buckets group data into fixed intervals so you can analyze trends across them. The following query uses [`lowerBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)) to place each trade into a 15-minute bucket, then calculates the average trade price and size for each ticker in each bucket:

```groovy test-set=6 order=avgByTimeBucket
avgByTimeBucket = tradingData.update("TimeBucket = lowerBin(Timestamp, 15 * MINUTE)").aggBy(
        [AggAvg("AvgPrice = Price"), AggAvg("AvgSize = Size")],
        "TimeBucket", "Ticker"
)
```

## Put it all together

The following example combines the business-hours [filter](../../reference/table-operations/filter/where.md) from [Filter and count with a calendar](#filter-and-count-with-a-calendar) with the daily summary from [Daily summaries](#daily-summaries), a combination common in financial data analysis. It reuses the `nyseCal` calendar and differs from those examples in two ways:

- Its data starts on January 1, an NYSE holiday, so the calendar [filter](../../reference/table-operations/filter/where.md) removes that whole day.
- Its query uses [`minuteOfDay`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#minuteOfDay(java.time.Instant,java.time.ZoneId,boolean)) to get each trade's minute of the day, then flags trades in the first hour after the open.

```groovy test-set=6 order=marketData,processedData,marketDailySummary
// Create synthetic market data: a row every 5 minutes, rotating among three tickers
marketData = emptyTable(1000).update(
        "Timestamp = '2024-01-01T09:00:00 ET' + ii * 5 * MINUTE",
        "Ticker = ii % 3 == 0 ? `AAPL` : (ii % 3 == 1 ? `GOOGL` : `MSFT`)",
        "Price = 150 + randomDouble(-50, 50)",
        "Size = randomInt(100, 10000)"
)

// Filter to business hours and add time-based features
// January 1 is an NYSE holiday, so the filter removes that whole day
processedData = marketData.where("nyseCal.isBusinessTime(Timestamp)").update(
        "Date = toLocalDate(Timestamp, 'ET')",
        "MinuteOfDay = minuteOfDay(Timestamp, 'ET', true)",
        // Before 10:30 AM (630 minutes after midnight): the first hour after the 9:30 AM open
        "IsEarlyTrading = MinuteOfDay < 630"
)

// Calculate daily summary statistics
marketDailySummary = processedData.aggBy(
        [AggCount("NumTrades"), AggSum("TotalSize = Size", "EarlyTrades = IsEarlyTrading"), AggAvg("AvgPrice = Price")],
        "Date", "Ticker"
)
```

## Next steps

To go further with time-series analysis, see:

- [Built-in time functions](../../reference/query-language/query-library/auto-imported/time.md)
- [Rolling window calculations](../../how-to-guides/rolling-aggregations.md)
- [Time-based joins](../../how-to-guides/joins-timeseries-range.md)
- [Work with time](../../conceptual/time-in-deephaven.md)
- [Work with calendars](../../how-to-guides/business-calendar.md)
