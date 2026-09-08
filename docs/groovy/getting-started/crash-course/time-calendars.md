---
title: Time and Calendars
---

This section covers working with time and calendars in Deephaven queries. Since Deephaven specializes in real-time operations, understanding how to work with time is crucial to building effective analyses.

Critical concepts covered in this crash course include:

- [Data types](#time-data-types)
- [Constants](#time-constants)
- [Creating temporal data](#create-temporal-data)
- [Time arithmetic](#time-arithmetic-and-filtering)
- [Time zones](#time-zones)
- [Business calendars](#business-calendars)
- [Common time operations](#common-time-operations)

## Time data types

Deephaven tables natively support all data types found in the [`java.time`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/package-summary.html) package.

Commonly used types include:

- [**`Instant`**](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Instant.html): A specific point in time.
- [**`ZonedDateTime`**](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/ZonedDateTime.html): A specific point in time with time zone information.
- [**`LocalDate`**](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html): A date without time zone information.
- [**`Duration`**](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Duration.html): A length of time; e.g. 5 minutes, 1 hour, etc.
- [**`Period`**](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Period.html): A date-based amount of time (like "2 months").

## Time constants

Deephaven offers a set of built-in constants for common time periods. All of the following values are expressed in nanoseconds:

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

You can create temporal data with [query string](./query-strings.md) methods. For instance, you can construct columns with a single timestamp, sequences of timestamps, and more:

```groovy test-set=2
temporalDataTable = emptyTable(10).update(
        "CurrentTime = now()",
        "SequentialSeconds = now() + ii * SECOND",
        "SequentialMinutes = now() + ii * MINUTE"
)
```

## Time arithmetic and filtering

Time arithmetic in tables should always be done with built-in methods. See [`DateTimeUtils`](https://docs.deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html) for the available constants and methods.

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

Filtering temporal data isn't much different from filtering numeric data:

```groovy test-set=3 order=timesGreaterThan,timesLessThan,timesInRange
timesGreaterThan = timeArithmeticTable.where("Timestamp >= '2025-06-01T09:34Z'")
timesLessThan = timeArithmeticTable.where("Timestamp < '2025-06-01T09:37Z'")
timesInRange = timeArithmeticTable.where(
        "Timestamp > '2025-06-01T09:34Z'",
        "Timestamp <= '2025-06-01T09:37Z'"
)
```

## Time zones

Time zones are a critical part of temporal data. For example, if it's 6 PM in Los Angeles, it's 3 AM the next day in Shanghai. There are many methods that require time zone information because the answer is dependent on the time zone.

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

All of the previous operations use [`Instant`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Instant.html) values, which contain no time zone information - hence the need to pass time zones like `'PT'` and `'Asia/Shanghai'`. Instead, you can bake the time zone information into the data by using [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/ZonedDateTime.html):

```groovy test-set=5
timeZoneArithmeticTable = emptyTable(10)
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

Business calendars help you work with trading days, business hours, and holidays. Deephaven comes with built-in calendars for major exchanges.

```groovy test-set=6
// The table will have 5 days' worth of trading data, one every 30 seconds
numTrades = 5 * 24 * 60 * 2

tradingData = emptyTable(numTrades).update(
        "Timestamp = '2025-07-14T09:30 ET' + ii * 30 * SECOND",
        "Ticker = (ii % 2 == 0) ? `A` : `B`",
        "Price = (ii % 2 == 0) ? randomDouble(50, 75) : randomDouble(120, 160)",
        "Size = randomDouble(0, 10)"
)
```

### Built-in calendars

Deephaven comes with a few built-in calendars.

> [!NOTE]
> The built-in calendars are examples. For any production system, a custom or other pre-defined calendar should be used.

```groovy test-set=6
import static io.deephaven.time.calendar.Calendars.calendar
import static io.deephaven.time.calendar.Calendars.calendarNames

println calendarNames()
nyseCal = calendar("USNYSE_EXAMPLE")
```

### Filter by business days and hours

A common use of calendars is to filter data based on whether or not it falls within business hours or on business days.

```groovy test-set=6 order=businessDaysOnly,businessHoursOnly,businessDayCounts
businessDaysOnly = tradingData.where("nyseCal.isBusinessDay(Timestamp)")
businessHoursOnly = tradingData.where("nyseCal.isBusinessTime(Timestamp)")

businessDayCounts = tradingData.update(
        "BizDaysFromStart = nyseCal.numberBusinessDates('2024-01-01T00:00:00 ET', Timestamp)"
)
```

## Common time operations

Aggregations over windows of time are essential for time-series analysis.

### Daily summaries

Combine time operations with aggregations for powerful analysis. For example, the following query calculates the daily sum of trades for each ticker:

```groovy test-set=6 order=dailySummary
dailySummary = tradingData
        .update("Date = toLocalDate(Timestamp, 'America/New_York')")
        .dropColumns("Timestamp")
        .sumBy("Date", "Ticker")
```

### Aggregations over time buckets

Data is commonly placed into temporal buckets, allowing for analysis of trends over specific intervals. The following query places each trade into a 15-minute bucket, then calculates the average trade price and size for each bucket:

```groovy test-set=6 order=avgByTimeBucket
import static io.deephaven.api.agg.Aggregation.AggAvg

avgByTimeBucket = tradingData.update("TimeBucket = lowerBin(Timestamp, 15 * MINUTE)").aggBy(
        [AggAvg("AvgPrice = Price"), AggAvg("AvgSize = Size")],
        "TimeBucket", "Ticker"
)
```

### Combine calendar and time methods

Here's a practical example that combines multiple time concepts:

```groovy test-set=7 order=marketData,processedData,dailySummary
import static io.deephaven.time.calendar.Calendars.calendar

// Get NYSE calendar
nyse = calendar("USNYSE_EXAMPLE")

// Create realistic market data
marketData = emptyTable(1000).update(
        "Timestamp = '2024-01-01T09:00:00 ET' + 'PT5m' * ii",
        "Symbol = i % 3 == 0 ? `AAPL` : (i % 3 == 1 ? `GOOGL` : `MSFT`)",
        "Price = 150 + randomDouble(-50, 50)",
        "Volume = randomInt(100, 10000)"
)

// Filter to business hours and add time-based features
processedData = marketData.where("nyse.isBusinessTime(Timestamp)").update(
        "Date = toLocalDate(Timestamp, 'America/New_York')",
        "MinutesSinceOpen = diffMinutes('2024-01-01T09:30:00 ET', Timestamp)",
        "IsEarlyTrading = MinutesSinceOpen <= 60"
)

// Calculate daily summary statistics
dailySummary = processedData.dropColumns("Timestamp").sumBy("Symbol", "Date")
```

This example demonstrates filtering by business hours, extracting time components, and performing time-based aggregations - all common patterns in financial data analysis.

## Next steps

Time handling is crucial for real-time data processing. The concepts covered here form the foundation for more advanced time-series analysis, including:

- [Built-in Time Functions](../../reference/query-language/query-library/auto-imported/index.md)
- [Rolling window calculations](../../how-to-guides/rolling-aggregations.md)
- [Time-based joins](../../how-to-guides/joins-timeseries-range.md)
- [Time in Deephaven](../../conceptual/time-in-deephaven.md)
- [Work with calendars](../../how-to-guides/business-calendar.md)
