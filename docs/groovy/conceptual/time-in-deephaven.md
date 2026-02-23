---
title: Work with time
sidebar_label: Work with time
---

This guide discusses working with time in Deephaven. Deephaven is a real-time data platform that handles streaming data of various formats and sizes, offering a unified interface for the ingestion, manipulation, and analysis of this data. A significant part of the Deephaven experience involves working with data that signifies specific moments in time: calendar dates, time-periods or durations, time zones, and more. These can appear as data in Deephaven tables, literals in query strings, or values in Groovy scripts. So, it is important to learn about the best tools for these different jobs.

Because time-based data types are integral to working with Deephaven, understanding their representations and how to manipulate them is critical. This will be true for both server-side applications and client-side applications. As such, the date-time types natively supported in Deephaven are a good starting point for this discussion.

## The Deephaven Query Language

All of Deephaven's natively-supported types are Java types, which integrate seamlessly with Deephaven's Groovy API. Java types can easily be created and manipulated with the Deephaven Query Language.

The Deephaven Query Language (DQL) is the primary way of expressing commands directly to the query engine. It is responsible for translating the user's intention into compiled code that the engine can execute. DQL is written with strings - called query strings - that can contain a mixture of Java and Groovy code. Thus, DQL query strings are the entry point to a universe of powerful built-in tools atop standard Java. These query strings are often used in the context of table operations, like creating new columns or applying filters. Here's a simple DQL example:

```groovy test-set=1
t = (
    emptyTable(10)
    .update("Col1 = ii", "Col2 = Col1 + 3", "Col3 = 2 * Math.sin(Col1 + Col2)")
    .where("Col1 % 2 == 0", "Col3 > 0")
)
```

> [!NOTE]
> To learn more about the details of the DQL syntax and exactly what these commands do, refer to the DQL section in our User Guide, which includes guides on [writing basic formulas](../how-to-guides/formulas.md), [working with strings](../how-to-guides/work-with-strings.md), [using built-in functions](../how-to-guides/built-in-functions.md), and more.

There are four important tools provided by DQL that are relevant to the discussion on date-time types.

### 1. Built-in Java functions

Deephaven has a collection of [built-in functions](../reference/query-language/formulas/auto-imported-functions.md) that are useful for working with date-time types. For the sake of performance, these functions are implemented in Java. DQL supports calling these functions directly in query strings, opening up all of Deephaven's [built-in Java functions](../reference/query-language/formulas/auto-imported-functions.md) to the Groovy interface. The following example uses the built-in Deephaven function [`now`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#now()) to get the current system time as a Java [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html):

```groovy test-set=2
t = emptyTable(5).update("CurrentTime = now()")
```

> [!NOTE]
> [`now`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#now()) uses the [current clock](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#currentClock()) of the Deephaven engine. This clock is typically the system clock, but it may be set to a simulated clock when replaying tables.

These [functions](../reference/query-language/formulas/auto-imported-functions.md) can also be applied to columns, constants, and variables. This slightly more complex example uses the built-in Deephaven function [`epochDaysToLocalDate`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochDaysToLocalDate(long)) to create a [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html) from a long that represents the number of days since the [Unix epoch](https://en.wikipedia.org/wiki/Unix_time):

```groovy test-set=3
t = emptyTable(5).update("DaysSinceEpoch = ii", "LocalDateColumn = epochDaysToLocalDate(DaysSinceEpoch)")
```

In addition to functions, Deephaven offers many [built-in constants](/core/javadoc/io/deephaven/time/DateTimeUtils.html) to simplify expressions involving time values. These constants, like [`SECOND`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#SECOND), [`DAY`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#DAY), and [`YEAR_365`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#YEAR_365), are equal to the number of nanoseconds in a given time period. Many of Deephaven's built-in functions operate at nanosecond resolution, so these constants provide a simple way to work with nanoseconds:

```groovy test-set=3
t = emptyTable(1).update(
        "CurrentTime = now()",
        "NanosSinceEpoch = epochNanos(CurrentTime)",
        "SecondsSinceEpoch = NanosSinceEpoch / SECOND",
        "MinutesSinceEpoch = NanosSinceEpoch / MINUTE",
        "DaysSinceEpoch = NanosSinceEpoch / DAY",
        "YearsSinceEpoch = NanosSinceEpoch / YEAR_AVG",
)
```

### 2. Java object methods

Both Groovy and Java are [object-oriented programming languages](https://docs.oracle.com/javase/tutorial/java/concepts/). As such, all Java objects have associated methods. These methods can be called in query strings. Here is an example that builds upon the previous example, and uses the [`getDayOfWeek`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/LocalDate.html#getDayOfWeek()) method bound to each [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html) object to extract the day of the week for each [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html):

```groovy test-set=4
t = emptyTable(5).update(
        "DaysSinceEpoch = ii",
        "LocalDateColumn = epochDaysToLocalDate(DaysSinceEpoch)",
        "DayOfWeek = LocalDateColumn.getDayOfWeek()",
)
```

To be clear:

- `DaysSinceEpoch` is a 64-bit integer.
- `LocalDateColumn` is a Java [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html) object.
- [`epochDaysToLocalDate`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochDaysToLocalDate(long)) is a Java function from the [built-in Deephaven library](../reference/query-language/formulas/auto-imported-functions.md).
- `DayOfWeek` is the return value of [`getDayOfWeek`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/LocalDate.html#getDayOfWeek()), a Java method bound to the [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html) class.

### 3. Arithmetic and inequality operators

Query strings support syntactic sugar for special operators such as `+`, `-`, `>`, `<`, `>=`, etc. for Java time types! For instance, it makes sense to add a [`Period`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Period.html) to an [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html), or a [`Duration`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Duration.html) to an [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html), or to multiply a [`Duration`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Duration.html) by an integer. This example uses the built-in [`parsePeriod`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parsePeriod(java.lang.String)) and [`parseDuration`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseDuration(java.lang.String)) functions to create period and duration columns from strings. Then, the overloaded addition operator `+` is used to add them to the `Timestamp` column, and the overloaded multiplication operator `*` is used to create a column with timestamps that increment daily:

```groovy test-set=5
t = emptyTable(5).update(
        "Timestamp = now()",
        "PeriodColumn = parsePeriod(`P1D`)",
        "DurationColumn = parseDuration(`PT24h`)",
        "TimePlusPeriod = Timestamp + PeriodColumn",
        "TimePlusDuration = Timestamp + DurationColumn",
        "IncreasingTime = Timestamp + PeriodColumn * i",
)
```

> [!NOTE]
> This example uses backticks to represent strings. For more info, see the guide on [working with strings](../how-to-guides/work-with-strings.md).

### 4. Date-times using DQL

In Deephaven, date-time values can be expressed using very simple [literal](https://en.wikipedia.org/wiki/Literal_(computer_programming)) syntax. These literal values can be used directly in query strings or as string inputs to [built-in functions](../reference/query-language/formulas/auto-imported-functions.md).

> [!TIP]
> In query strings, time literals are denoted with _single quotes_.

This example creates [`Duration`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Duration.html) columns from a time literal as well as from a string parsed by [`parseDuration`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseDuration(java.lang.String)):

```groovy test-set=6
t = emptyTable(5).update("DurationFromLiteral = 'PT24h'", "DurationFromString = parseDuration(`PT24h`)")
```

> [!NOTE]
> Note the difference in single quotes for the time literal and back quotes for the string.

Using query string time literals can yield more compact and more efficient code. In the prior example, ``parseDuration(`PT24h`)`` is evaluated for every single row in the table, but here `'PT24h'` is only evaluated once for the entire table. This can lead to massive performance differences for large tables:

```groovy test-set=7, order=t1,t2 default=:log
t1Start = System.nanoTime() / 1000000000
t1 = emptyTable(100000000).update("DurationColumn = parseDuration(`PT24h`)")
t1End = System.nanoTime() / 1000000000

t2Start = System.nanoTime() / 1000000000
t2 = emptyTable(100000000).update("DurationColumn = 'PT24h'")
t2End = System.nanoTime() / 1000000000

t1Time = t1End - t1Start
t2Time = t2End - t2Start
println "Using built-in parse function: " + t1Time + " seconds."
println "Using literal: " + t2Time + " seconds."
```

Most of the seven key Java types can be created using literals or functions like [`parseDuration`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseDuration(java.lang.String)):

```groovy test-set=7 order=t1,t2,t3,t4,t5,t6,t7
// ZoneId columns can be created with literals or the parseTimeZone built-in.
// The literal or string argument must be a valid Java time zone.
t1 = emptyTable(1).update("TimeZone1 = 'GMT'", "TimeZone2 = parseTimeZone(`America/New_York`)")

// LocalDate columns can be created with literals or the parseLocalDate built-in.
// The literal or string argument must use the ISO-8601 date format 'YYYY-MM-DD'.
t2 = emptyTable(1).update("LocalDate1 = '2022-03-28'", "LocalDate2 = parseLocalDate(`2075-08-08`)")

// LocalTime columns can be created with literals or the parseLocalTime built-in.
// The literal or string argument must use the ISO-8601 time format 'hh:mm:ss[.SSSSSSSSS]'.
t3 = emptyTable(1).update("LocalTime1 = '10:15:30.46'", "LocalTime2 = parseLocalTime(`12:01:01.4567`)")

// Instant columns can be created with literals or the parseInstant built-in.
// The literal or string arguments must use the ISO-8601 date-time format `yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ`.
t4 = emptyTable(1).update(
        "Instant1 = '2015-01-30T12:34:56Z'",
        "Instant2 = parseInstant(`2023-11-21T21:30:45Z`)",
)

// ZonedDateTime columns cannot be created with literals, as they are indistinguishable from Instant literals.
// ZonedDateTime columns can be created with the parseZonedDateTime built-in,
// or by localizing the time zone of an Instant with the atZone() method.
// The string arguments must use the ISO-8601 date-time format `yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ`.
t5 = emptyTable(1).update(
        "ZonedDateTime1 = parseZonedDateTime(`2021-11-03T01:02:03 GMT`)",
        "ZonedDateTime2 = '2023-11-21T21:30:45 GMT'.atZone('GMT')",
)

// Duration columns can be created with literals or the parseDuration built-in.
// The literal or string arguments must use the ISO-8601 duration format 'PnDTnHnMn.nS'.
// Negative durations are represented as 'P-nDT-nH-nM-n.nS' or '-PnDTnHnMn.nS'.
t6 = emptyTable(1).update("Duration1 = 'PT6H30M30S'", "Duration2 = parseDuration(`PT10H`)")

// Period columns can be created with literals or the parsePeriod built-in.
// The literal or string arguments must use the ISO-8601 period format 'PnYnMnD'.
// Negative periods are represented as 'P-nY-nM-nD' or '-PnYnMnD'.
t7 = emptyTable(1).update("Period1 = 'P2Y3M10D'", "Period2 = parsePeriod(`P10Y0M3D`)")
```

### Put it all together

To illustrate the power and ease of working with natively supported date-times, this example uses time literals, operator-overloaded arithmetic, and Java time methods together to create timestamps, compute time differences, and extract information about those timestamps in Tokyo and New York time zones:

```groovy test-set=9 order=t,tMeta
// Create reference time and timestamp column using operator overloading and Java method multipliedBy()
t = emptyTable(100).update(
        "Reference = '2025-10-15T13:23 ET'",
        "Timestamp = Reference + 'PT1h'.multipliedBy(ii)",
)

// create a simple closure
f = { java.time.Instant a, java.time.Duration b -> a - b }

// Use operator overloading and diffMinutes() built-in to get time since reference time
t = t.update(
        "DifferenceNanos = Timestamp - Reference",
        "DifferenceMin = diffMinutes(Reference, Timestamp)",
)

// Finally, use built-in functions and time zone literals to get date and
// day of week in two different time zones
t = t.update(
        "DateTokyo = toLocalDate(Timestamp, 'Asia/Tokyo')",
        "DayTokyo = dayOfWeek(Timestamp, 'Asia/Tokyo')",
        "DateNY = toLocalDate(Timestamp, 'America/New_York')",
        "DayNY = dayOfWeek(Timestamp, 'America/New_York')",
        "TimeMinus1H = f(Timestamp, 'PT1h')",
)

// Assess metadata to see that types are as expected
tMeta = t.meta()
```

Hopefully, it's apparent that working with date-times in Deephaven queries is a breeze. With all of the [built-in time functions](/core/javadoc/io/deephaven/time/DateTimeUtils.html), most date-time operations can be accomplished with native Deephaven operations.

## Natively supported date-time types

The Deephaven query engine is responsible for executing a query and updating the query results as data changes. The engine is implemented in Java. As a result, all of the date-time types that are _natively_ supported by the Deephaven engine are Java types. Java's [`java.time`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/package-summary.html) package provides types for representing time-related data.

The [`java.time`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/package-summary.html) types natively supported by Deephaven are:

| Type                                                                                                                    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ----------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`java.time.ZoneId`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/ZoneId.html)               | A [`ZoneId`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/ZoneId.html) represents a time zone such as "Europe/Paris" or "CST".                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| [`java.time.LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html)         | A [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html) is a date _without_ a time zone in the [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) system, such as "2007-12-03" or "2057-01-28".                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| [`java.time.LocalTime`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalTime.html)         | [`LocalTime`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalTime.html) is a timestamp _without_ a date or time zone in the [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) system, such as "10:15:30", "13:45:30.123456789". [`LocalTime`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalTime.html) has nanosecond resolution.                                                                                                                                                                                                                                                                                                                                            |
| [`java.time.Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html)              | An [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) represents an unambiguous specific point on the timeline, such as `2021-04-12T14:13:07 UTC`. [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) has nanosecond resolution.                                                                                                                                                                                                                                                                                                                                                                                                               |
| [`java.time.ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/ZonedDateTime.html) | A [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/ZonedDateTime.html) represents an unambiguous specific point on the timeline with an associated time zone, such as `2021-04-12T14:13:07 America/New_York`. [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/ZonedDateTime.html) has nanosecond resolution. A [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/ZonedDateTime.html) is effectively an [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) with a [`ZoneId`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/ZoneId.html). |
| [`java.time.Duration`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Duration.html)           | A [`Duration`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Duration.html) represents a _duration_ of time, such as "5.5 seconds". [`Duration`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Duration.html) has nanosecond resolution.                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| [`java.time.Period`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Period.html)               | A [`Period`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Period.html) is a date-based amount of time in the [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) system, such as "P2y3m4d" (2 years, 3 months, and 4 days).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |

### Specific date-times

Deephaven uses [`java.time.Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) to represent specific points in time in queries. An Instant is accurate to the nearest nanosecond.

To see the current date and time on your system, run the following:

```groovy
println now()
```

This will print out a date and time in the default format, such as `2021-09-09T11:58:41.041000000 ET`. The default format consists of the following fields:

`yyyy-MM-ddThh:mm:ss.ffffff TZ`

- `yyyy` - the year
- `MM` - the month
- `dd` - the day
- `T` - the separator between the date and time
- `hh` - the hour of the day
- `mm` - the minute of the hour
- `ss` - the second of the minute.
- `ffffff` - the fraction of a second.
- `TZ` - the time zone.

Deephaven stores [dates-times](../reference/query-language/types/date-time.md) using the [`java.time.Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) class. Internally, this stores the date-time as a signed 64-bit long, which contains the number of nanoseconds since the Unix epoch (January 1, 1970, 00:00:00 GMT). You can create these directly (like in the code [above](#specific-date-times)) and use dates and times directly in the query language, including adding and subtracting them.

The following example shows the creation of two specific points in time, `time1` and `time2`, exactly one year apart from each other. We then calculate the difference between these two date/time instances, resulting in 31,536,000,000,000,000 nanos (there are 31,536,000 seconds in a year).

```groovy
time1 = parseInstant("2020-08-01T12:00:00 ET")
time2 = parseInstant("2021-08-01T12:00:00 ET")

timeDiff = diffNanos(time2, time1)
println timeDiff
```

### Periods and Durations

Periods and durations represent spans of time that can be either positive or negative. A period represents a span of time that is greater than one day - lengths of time one would normally count on a calendar. A duration represents a span of time that is less than one day in length - spans of time one would normally count on a clock (but accurate down to the millisecond!). Deephaven uses Java's [`Period`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html) [`Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html) classes.

Durations are prefixed by a `PT`, whereas Periods are prefixed by the letter `P`.

The following query demonstrates several query operations using periods and durations.

> [!NOTE]
> In the example below, we've set the default format for `Timestamp` columns to `YYYY-MM-DDThh:mm:ss.fff`. See [How to set date-time format](../how-to-guides/set-date-time-format.md) to learn how.

```groovy ticking-table order=source,result1,result2,result3,result4
base_time = parseInstant("2023-01-01T00:00:00 UTC")

source = emptyTable(10).update("Timestamp = base_time + i * SECOND")
hour_duration = parseDuration("PT1H")
day_period = parsePeriod("P1D")
result1 = source.update("TS2 = plus(Timestamp, hour_duration)")
result2 = source.update("TS2 = plus(Timestamp, day_period)")
result3 = source.update("TS2 = Timestamp + 'PT1H'")
result4 = source.update("TS2 = Timestamp + 'P-1D'")
```

The above example creates a `source` table from an [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) called `base_time`. Each row of the `source` table is one second after the previous. A one-hour duration and a one-day period are then instantiated. From there, four resultant tables are derived from `source`:

- `result1` uses the query language's built-in `plus` method to add `hour_duration` to `Timestamp`, so the `TS2` column contains timestamps one hour after those in the `Timestamp` column.
- `result2` uses the same built-in `plus` method, but adds `day_period` instead. Thus, `TS2` contains timestamps one day after those in the `Timestamp` column.
- `result3` uses the overloaded `+` operator to add a one hour duration to `Timestamp` again. Thus, `TS2` looks identical to that in the `result1` table.
- `result4` uses the overloaded `+` operator to add a negative day-long period to `Timestamp`. Thus, `TS2` looks identical to that in the `result2` table.

Durations use the following suffixes for different units of time:

- `-`: Negative
- `H`: Hour
- `M`: Minute
- `S`: Second

Periods use the following suffixes for different units of time:

- `-` - Negative
- `Y` - Year
- `M` - Month
- `D` - Day

For example, a duration of `PT1H1M-1S` means one second short of one hour and one minute. Equivalently, a period of `P1Y1M-1D` means one day short of one year and one month.

The following example creates an instant, then adds a positive and negative period and duration.

```groovy order=:log
time1 = parseInstant("2020-04-01T12:00:00 ET")

posPeriod = parsePeriod("P5D")
negPeriod = parsePeriod("P-5D")
posDuration = parseDuration("PT1H1M1S")
negDuration = parseDuration("PT-5H1M5S")


println plus(time1, posPeriod)
println plus(time1, negPeriod)
println plus(time1, posDuration)
println plus(time1, negDuration)
```

### Time zones

Deephaven uses Java's [`TimeZone`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/TimeZone.html) class to store time zone information. Time zones in instants use abbreviations, such as `ET` for US Eastern time, `UTC` or `Z` for coordinated universal time (UTC), etc.

```groovy order=:log
println timeZone("ET")
println timeZone("PT")
println timeZone("UTC")
```

By default, Deephaven uses the US East time zone (`"America/New_York"`) as the time zone for database operations. For example, when writing a CSV from a table which includes [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) columns, the New York time zone will be used unless a different time zone is passed in to the [`write_csv`](../reference/data-import-export/CSV/writeCsv.md) call.

The following example prints a time in the Denver time zone and in the New York time zone. The printed values end with `Z` (UTC), so the two-hour difference between the two time zones is apparent.

```groovy order=:log
time1 = parseInstant("2021-01-01T01:00 ET")
time2 = parseInstant("2021-01-01T01:00 MT")

println time1
println time2
```

## Predefined date-time variables

Deephaven provides several predefined date/time variables representing specified time periods in nanoseconds. These can be used in formulas.

Predefined [date-time](../reference/query-language/types/date-time.md) variables are:

- `YEAR_365`
- `YEAR_AVG`
- `WEEK`
- `DAY`
- `HOUR`
- `MINUTE`
- `SECOND`

The following example shows these variables and how to use them, both in a simple calculation and when manipulating a table. Notice that the timestamps in TS2 are one minute after the timestamps in the original table. A user will not typically call [`plus`](../reference/time/datetime/plus.md) directly; the Deephaven Query Language will do that automatically when times are summed, as in the `source`/`result` part of the example.

```groovy order=source,result
println YEAR_365
println YEAR_AVG
println WEEK
println DAY
println HOUR
println MINUTE
println SECOND

time1 = parseInstant("2020-04-01T12:00:00 ET")
time2 = plus(time1, WEEK)
println time1
println time2

source = timeTable("PT1S")

result = source.update("TS2 = Timestamp + MINUTE")
```

## Examples

This section contains an example set demonstrating the date and time methods described above.

> [!NOTE]
> The examples in this section depend on the `events` table.

First, we create a simple table with four rows. Note that all of our times are based in NY.

```groovy test-set=1 order=events
time1 = parseInstant("2020-04-01T09:00:00 ET")
time2 = parseInstant("2020-04-01T10:28:32 ET")
time3 = parseInstant("2020-04-01T12:00:00 ET")
time4 = parseInstant("2020-04-01T16:59:59 ET")

events = newTable(
   instantCol("EventDateTime", time1, time2, time3, time4),
   stringCol("Level", "Info", "Error", "Warning", "Info"),
   stringCol("Event", "System starting", "Something bad happened", "Invalid login", "System stopping"),
)
```

### Filter to events after a certain time

The following query returns all events after 10AM 2020-04-01. Note that comparison [operators](../how-to-guides/operators.md) on [date-times](../reference/query-language/types/date-time.md) are supported in query language strings. To use a comparison operator on a date-time string, the date-time string must be wrapped in single quotes `’`.

```groovy test-set=1 order=eventsAfterTen
eventsAfterTen = events.where("EventDateTime > '2020-04-01T10:00 ET'")
```

> [!NOTE]
> To construct date-times in DQL, use the single quote.

### Filter to events within a range

The following example returns all events between 10AM - 4PM on 2020-04-01, using a [formula](../how-to-guides/formulas.md) within the query [string](../reference/query-language/types/strings.md).

```groovy test-set=1 order=eventsMiddleDay1
eventsMiddleDay1 = events.where("EventDateTime >= '2020-04-01T10:00 ET' && EventDateTime <= '2020-04-01T16:00 ET'")
```

The following example returns all dates between 10AM - 4PM on 2020-04-01, using the `inRange` method.

```groovy test-set=1 order=eventsMiddleDay2
eventsMiddleDay2 = events.where("inRange(EventDateTime, '2020-04-01T10:00 ET', '2020-04-01T16:00 ET')")
```

## Related documentation

- [How to use filters](../how-to-guides/filters.md)
- [How to work with strings](../how-to-guides/work-with-strings.md)
- [date-time](../reference/query-language/types/date-time.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
