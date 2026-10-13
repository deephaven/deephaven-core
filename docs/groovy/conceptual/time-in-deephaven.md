---
title: Work with time
---

This guide shows how to work with dates, times, durations, periods, and time zones in Deephaven. Deephaven is a real-time data platform that handles streaming data of various formats and sizes, with a unified interface for ingesting, manipulating, and analyzing it. Much of that data marks specific moments in time, so date-times are central to working in Deephaven. These values appear as data in tables, as literals in query strings, and as values in Groovy scripts.

This guide covers the Java date-time types the engine supports natively, the query-language tools for working with them, filtering tables by time, and using date-times directly in Groovy scripts.

## Natively supported date-time types

The Deephaven query engine is responsible for executing a query and updating the query results as data changes. The engine is implemented in Java. As a result, all the date-time types the Deephaven engine supports _natively_ are Java types. Java's [`java.time`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/package-summary.html) package provides types for representing time-related data.

This guide focuses on seven [`java.time`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/package-summary.html) types that Deephaven supports natively. The rest of this guide calls them the key Java types:

| Type                                                                                                                   | Description                                                                                                                                                                                                                                                        |
| ---------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`java.time.ZoneId`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZoneId.html)               | A `ZoneId` represents a time zone such as `Europe/Paris` or `America/Chicago`.                                                                                                                                                                                     |
| [`java.time.LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html)         | A `LocalDate` is a date _without_ a time zone in the [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) system, such as `2007-12-03` or `2057-01-28`.                                                                                                              |
| [`java.time.LocalTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalTime.html)         | A `LocalTime` is a time of day _without_ a date or time zone in the [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) system, such as `10:15:30` or `13:45:30.123456789`. `LocalTime` has nanosecond resolution.                                                  |
| [`java.time.Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html)             | An `Instant` represents an unambiguous specific point on the timeline, such as `2021-04-12T14:13:07 UTC`. `Instant` has nanosecond resolution.                                                                                                                     |
| [`java.time.ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZonedDateTime.html) | A `ZonedDateTime` represents an unambiguous specific point on the timeline with an associated time zone, such as `2021-04-12T14:13:07 America/New_York`. `ZonedDateTime` has nanosecond resolution. A `ZonedDateTime` is effectively an `Instant` with a `ZoneId`. |
| [`java.time.Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html)           | A `Duration` represents a _duration_ of time, such as "5.5 seconds". `Duration` has nanosecond resolution.                                                                                                                                                         |
| [`java.time.Period`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html)               | A `Period` is a date-based amount of time in the [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) system, such as `P2Y3M4D` (2 years, 3 months, and 4 days).                                                                                                     |

Periods and durations represent spans of time that can be either positive or negative. A period is a date-based amount of time, measured in years, months, weeks, and days. Periods are the spans you count on a calendar. A duration is a time-based amount of time, measured in days, hours, minutes, seconds, and fractions of a second down to the nanosecond. Durations are the spans you count on a clock, and a day in a duration is always exactly 24 hours.

## The Deephaven Query Language

All of Deephaven's natively supported types are Java types. Groovy runs on the Java Virtual Machine, so Groovy scripts can use these types directly.

The Deephaven Query Language (DQL) is the primary way of expressing commands directly to the query engine. It is responsible for translating the user's intention into compiled code that the engine can execute. You write DQL as strings, called query strings, that can contain a mixture of Java and Groovy code. Query strings give you access to Deephaven's built-in functions. You typically use query strings in table operations, such as creating columns or filtering rows. Here's a simple DQL example:

```groovy test-set=1
t = (
    emptyTable(10)
    .update("Col1 = ii", "Col2 = Col1 + 3", "Col3 = 2 * Math.sin(Col1 + Col2)")
    .where("Col1 % 2 == 0", "Col3 > 0")
)
```

`ii` and `i` are [special variables](../reference/query-language/variables/special-variables.md) that hold each row's index, as a `long` and an `int` respectively.

> [!NOTE]
> To learn more about the details of the DQL syntax and exactly what these commands do, refer to the DQL section in our User Guide, which includes guides on [writing basic formulas](../how-to-guides/formulas.md), [working with strings](../how-to-guides/work-with-strings.md), [using built-in functions](../how-to-guides/built-in-functions.md), and more.

DQL provides four tools for working with date-time types.

### 1. Built-in Java functions

Deephaven has a collection of [built-in functions](../reference/query-language/query-library/auto-imported/index.md) that are useful for working with date-time types. For the sake of performance, these functions are implemented in Java. You can call these functions directly in query strings and in Groovy scripts. The following example uses the built-in Deephaven function [`now`](../reference/time/datetime/now.md) to get the current time as a Java [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html):

```groovy test-set=2
t = emptyTable(5).update("CurrentTime = now()")
```

> [!NOTE]
> [`now`](../reference/time/datetime/now.md) uses the [current clock](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#currentClock()) of the Deephaven engine. To learn when that clock differs from the system clock, see [Current time and the Deephaven clock](#current-time-and-the-deephaven-clock).

You can also apply these [functions](../reference/query-language/query-library/auto-imported/index.md) to columns, constants, and variables. This slightly more complex example uses the built-in Deephaven function [`epochDaysToLocalDate`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochDaysToLocalDate(long)) to create a [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html) from a `long` that represents the number of days since the [Unix epoch](https://en.wikipedia.org/wiki/Unix_time):

```groovy test-set=3
t = emptyTable(5).update("DaysSinceEpoch = ii", "LocalDateColumn = epochDaysToLocalDate(DaysSinceEpoch)")
```

In addition to functions, Deephaven offers many [built-in constants](/core/javadoc/io/deephaven/time/DateTimeUtils.html) for expressions involving time values. Many built-in functions work in nanoseconds, and these constants convert between nanoseconds and larger units:

- [`MICRO`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#MICRO), [`MILLI`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#MILLI), [`SECOND`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#SECOND), [`MINUTE`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#MINUTE), [`HOUR`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#HOUR), [`DAY`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#DAY), [`WEEK`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#WEEK), and [`YEAR_365`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#YEAR_365) each equal the number of nanoseconds in that unit.
- [`YEAR_AVG`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#YEAR_AVG) equals the number of nanoseconds in an average Gregorian year.
- Reciprocal constants such as [`SECONDS_PER_NANO`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#SECONDS_PER_NANO) convert nanoseconds to larger units.

The following example divides a nanosecond count by several of these constants:

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

Java is an [object-oriented programming language](https://docs.oracle.com/javase/tutorial/java/concepts/), so every Java object has methods. You can call those methods in query strings. This example extends the [`epochDaysToLocalDate`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochDaysToLocalDate(long)) example from [Built-in Java functions](#1-built-in-java-functions), which turns a count of days since the Unix epoch into a [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html). It uses the [`getDayOfWeek`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html#getDayOfWeek()) method of each `LocalDate` object to extract the day of the week:

```groovy test-set=4
t = emptyTable(5).update(
        "DaysSinceEpoch = ii",
        "LocalDateColumn = epochDaysToLocalDate(DaysSinceEpoch)",
        "DayOfWeek = LocalDateColumn.getDayOfWeek()",
)
```

To be clear:

- `DaysSinceEpoch` is a 64-bit integer.
- `LocalDateColumn` is a Java [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html) object.
- [`epochDaysToLocalDate`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochDaysToLocalDate(long)) is a Java function from the [built-in Deephaven library](../reference/query-language/query-library/auto-imported/index.md).
- `DayOfWeek` is the return value of [`getDayOfWeek`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html#getDayOfWeek()), a Java method bound to the [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html) class.

### 3. Arithmetic and comparison operators

Query strings support arithmetic operators such as `+`, `-`, and `*`, and comparison operators such as `<`, `<=`, `>`, `>=`, and `==`, for Java time types. For instance, you can add a [`Period`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html) or a [`Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html) to an [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html), or multiply a `Duration` or a `Period` by an integer. This example uses the built-in [`parsePeriod`](../reference/time/datetime/parsePeriod.md) and [`parseDuration`](../reference/time/datetime/parseDuration.md) functions to create period and duration columns from strings. The overloaded `+` operator then adds them to the `Timestamp` column, and the overloaded `*` operator creates a column of timestamps that increment daily:

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

An [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) has no time zone. You can add a [`Period`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html) to an `Instant` only when the period is in days or weeks. Each of those days counts as exactly 24 hours, as it does in a [`Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html). To add months or years, or days that follow daylight-saving changes, add the period to a [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZonedDateTime.html) instead.

The `+` operator in a query string calls the built-in [`plus`](../reference/time/datetime/plus.md) function, so you rarely need to call `plus` directly. The following query compares the two. It also uses `+` to add a negative period [literal](#4-date-time-literals) and a [built-in constant](#1-built-in-java-functions).

```groovy order=source,result1,result2,result3,result4,result5
baseTime = parseInstant("2023-01-01T00:00:00 UTC")

source = emptyTable(10).update("Timestamp = baseTime + i * SECOND")
hourDuration = parseDuration("PT1H")
dayPeriod = parsePeriod("P1D")
result1 = source.update("TS2 = plus(Timestamp, hourDuration)")
result2 = source.update("TS2 = plus(Timestamp, dayPeriod)")
result3 = source.update("TS2 = Timestamp + 'PT1H'")
result4 = source.update("TS2 = Timestamp + 'P-1D'")
result5 = source.update("TS2 = Timestamp + MINUTE")
```

The above example creates a `source` table from an [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) called `baseTime`. Each row of the `source` table is one second after the previous. The example then creates a one-hour duration and a one-day period, and derives five tables from `source`:

- `result1` uses the query language's built-in [`plus`](../reference/time/datetime/plus.md) function to add `hourDuration` to `Timestamp`, so the `TS2` column contains timestamps one hour after those in the `Timestamp` column.
- `result2` uses the same built-in [`plus`](../reference/time/datetime/plus.md) function, but adds `dayPeriod` instead. Thus, `TS2` contains timestamps one day after those in the `Timestamp` column.
- `result3` uses the overloaded `+` operator to add a one-hour duration to `Timestamp` again. Thus, `TS2` looks identical to that in the `result1` table.
- `result4` uses the overloaded `+` operator to add a negative day-long period to `Timestamp`. Thus, `TS2` contains timestamps one day before those in the `Timestamp` column.
- `result5` uses the overloaded `+` operator to add the [`MINUTE`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#MINUTE) constant, which is a number of nanoseconds, to `Timestamp`. Thus, `TS2` contains timestamps one minute after those in the `Timestamp` column.

### 4. Date-time literals

You can write date-time values as [literals](https://en.wikipedia.org/wiki/Literal_(computer_programming)) directly in query strings. The same text, written as a backtick string, can be passed to [built-in](../reference/query-language/query-library/auto-imported/index.md) parse functions such as `parseDuration`.

> [!TIP]
> In query strings, time literals are denoted with _single quotes_, and strings with backticks.

This example creates [`Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html) columns from a time literal as well as from a string parsed by [`parseDuration`](../reference/time/datetime/parseDuration.md):

```groovy test-set=6
t = emptyTable(5).update("DurationFromLiteral = 'PT24h'", "DurationFromString = parseDuration(`PT24h`)")
```

Using query string time literals can yield more compact and more efficient code. In the prior example, the engine evaluates ``parseDuration(`PT24h`)`` once per row but evaluates the literal `'PT24h'` once for the whole table. On large tables, the literal is much faster:

```groovy test-set=7 order=t1,t2 default=:log
t1Start = System.nanoTime() / 1000000000
t1 = emptyTable(10_000_000).update("DurationColumn = parseDuration(`PT24h`)")
t1End = System.nanoTime() / 1000000000

t2Start = System.nanoTime() / 1000000000
t2 = emptyTable(10_000_000).update("DurationColumn = 'PT24h'")
t2End = System.nanoTime() / 1000000000

t1Time = t1End - t1Start
t2Time = t2End - t2Start
println "Using built-in parse function: " + t1Time + " seconds."
println "Using literal: " + t2Time + " seconds."
```

You can create most of the seven key Java types with literals or with functions like [`parseDuration`](../reference/time/datetime/parseDuration.md):

```groovy test-set=7 order=t1,t2,t3,t4,t5,t6,t7
// ZoneId columns can be created with literals or the parseTimeZone built-in.
// The literal or string argument must be a valid Java time zone.
t1 = emptyTable(1).update("TimeZone1 = 'GMT'", "TimeZone2 = parseTimeZone(`America/New_York`)")

// LocalDate columns can be created with literals or the parseLocalDate built-in.
// The literal or string argument must use the ISO-8601 date format 'yyyy-MM-dd'.
t2 = emptyTable(1).update("LocalDate1 = '2022-03-28'", "LocalDate2 = parseLocalDate(`2075-08-08`)")

// LocalTime columns can be created with literals or the parseLocalTime built-in.
// The literal or string argument must use the ISO-8601 time format 'HH:mm[:ss[.SSSSSSSSS]]'.
t3 = emptyTable(1).update("LocalTime1 = '10:15:30.46'", "LocalTime2 = parseLocalTime(`12:01:01.4567`)")

// Instant columns can be created with literals or the parseInstant built-in.
// The literal or string arguments must include a time zone, as in `yyyy-MM-ddTHH:mm[:ss[.SSSSSSSSS]] TZ` or with a trailing `Z` for UTC.
t4 = emptyTable(1).update(
        "Instant1 = '2015-01-30T12:34:56Z'",
        "Instant2 = parseInstant(`2023-11-21T21:30:45Z`)",
)

// ZonedDateTime columns cannot be created with literals, as they are indistinguishable from Instant literals.
// ZonedDateTime columns can be created with the parseZonedDateTime built-in,
// or by localizing the time zone of an Instant with the atZone() method.
// The string arguments must use the ISO-8601 date-time format `yyyy-MM-ddTHH:mm[:ss[.SSSSSSSSS]] TZ`.
t5 = emptyTable(1).update(
        "ZonedDateTime1 = parseZonedDateTime(`2021-11-03T01:02:03 GMT`)",
        "ZonedDateTime2 = '2023-11-21T21:30:45 GMT'.atZone('GMT')",
)

// Duration columns can be created with literals or the parseDuration built-in.
// The literal or string arguments must use the ISO-8601 duration format 'PnDTnHnMn.nS'.
// Negative durations are represented as 'P-nDT-nH-nM-n.nS' or '-PnDTnHnMn.nS'.
t6 = emptyTable(1).update("Duration1 = 'PT6H30M30S'", "Duration2 = parseDuration(`PT10H`)")

// Period columns can be created with literals or the parsePeriod built-in.
// The literal or string arguments must use the ISO-8601 period format 'PnYnMnWnD', omitting any unused unit.
// Negative periods are represented as 'P-nY-nM-nD' or '-PnYnMnD'.
t7 = emptyTable(1).update("Period1 = 'P2Y3M10D'", "Period2 = parsePeriod(`P10Y0M3D`)")
```

Duration and period strings both start with `P`. A duration uses `T` to separate its day part from its time part, as in `PnDTnHnMn.nS`. A period has no time part: it uses the format `PnYnMnWnD`, where you can leave out any unit you don't need, as in `P2Y3M` or `P4W`.

Durations use the following letters for different units of time:

- `D`: Day (always 24 hours), before the `T`
- `H`: Hour
- `M`: Minute
- `S`: Second

Periods use the following letters for different units of time:

- `Y`: Year
- `M`: Month
- `W`: Week
- `D`: Day

A `-` sign can come before the whole string, which negates every component, or before any single component. For example, a duration of `PT1H1M-1S` means one second short of one hour and one minute. Similarly, a period of `P1Y1M-1D` means one day short of one year and one month.

### Put it all together

To illustrate the power and ease of working with natively supported date-times, this example combines time literals, overloaded arithmetic operators, and Java time methods to create timestamps, compute time differences, and extract dates and days of the week in the Tokyo and New York time zones. `ET` is Deephaven's built-in alias for `America/New_York`. For more about aliases, see [Time zones and aliases](#time-zones-and-aliases).

```groovy test-set=9 order=t,tMeta
// Create reference time and timestamp column using operator overloading and Java method multipliedBy()
t = emptyTable(100).update(
        "Reference = '2025-10-15T13:23 ET'",
        "Timestamp = Reference + 'PT1h'.multipliedBy(ii)",
)

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
)

// Assess metadata to see that types are as expected
tMeta = t.meta()
```

Hopefully, it's apparent that working with date-times in Deephaven queries is a breeze. The [built-in time functions](/core/javadoc/io/deephaven/time/DateTimeUtils.html) cover most date-time operations in query strings.

## Filter by time

This section filters an `events` table by time. It uses the date-time literals and comparison operators introduced in [The Deephaven Query Language](#the-deephaven-query-language).

The following code creates an `events` table with four rows. All times use the `ET` (US Eastern) time zone.

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

The following query returns all events after 10 AM on 2020-04-01. Query strings support comparison [operators](../how-to-guides/operators.md) on [date-times](../reference/query-language/types/date-time.md). To compare a date-time column with a fixed date-time, write the date-time as a literal in single quotes (`'`).

```groovy test-set=1 order=eventsAfterTen
eventsAfterTen = events.where("EventDateTime > '2020-04-01T10:00 ET'")
```

### Filter to events within a range

The following example returns all events between 10 AM and 4 PM on 2020-04-01 by combining two comparisons with `&&` in one query string.

```groovy test-set=1 order=eventsMiddleDay1
eventsMiddleDay1 = events.where("EventDateTime >= '2020-04-01T10:00 ET' && EventDateTime <= '2020-04-01T16:00 ET'")
```

The following example returns the same events with the [`inRange`](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#inRange(T,T,T)) function.

```groovy test-set=1 order=eventsMiddleDay2
eventsMiddleDay2 = events.where("inRange(EventDateTime, '2020-04-01T10:00 ET', '2020-04-01T16:00 ET')")
```

## Date-times in Groovy scripts

Groovy runs on the Java Virtual Machine, so Groovy scripts use the same key Java types as query strings, with no conversion between them. You can call built-in functions such as [`now`](../reference/time/datetime/now.md), [`parseInstant`](../reference/time/datetime/parseInstant.md), and [`plus`](../reference/time/datetime/plus.md) in scripts as well as in query strings. For example, `plus(parseInstant("2020-04-01T12:00:00 ET"), parsePeriod("P1D"))` in a script adds a one-day period the same way `result2` does in [Arithmetic and comparison operators](#3-arithmetic-and-comparison-operators).

### Current time and the Deephaven clock

To see the current date and time, run the following:

```groovy order=:log
println now()
```

[`now`](../reference/time/datetime/now.md) and [`today`](../reference/time/datetime/today.md) return current date-time information according to the _Deephaven clock_. By default, the Deephaven clock is the system clock of the computer running Deephaven. [`DateTimeUtils.setClock`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#setClock(io.deephaven.base.clock.Clock)) replaces it with a custom clock, such as a simulated clock. When that happens, `now` and `today` return times from that clock instead of the system clock. [Replaying historical data](../how-to-guides/replay-data.md) does not change the Deephaven clock: the replayer drives its replayed tables from its own simulated clock.

### Differences between instants

An [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) represents a point in time with no time zone attached. In table columns, Deephaven stores each `Instant` as a signed 64-bit count of nanoseconds since the Unix epoch (January 1, 1970, 00:00:00 UTC). You can create `Instant` values in a script, add durations and periods to them, and subtract one `Instant` from another, both in scripts and in query strings.

The following example creates two `Instant` values, `time1` and `time2`, exactly one 365-day year apart. [`diffNanos`](../reference/time/datetime/diffNanos.md) returns their difference, 31,536,000,000,000,000 nanoseconds, because a 365-day year has 31,536,000 seconds.

```groovy order=:log
time1 = parseInstant("2020-08-01T12:00:00 ET")
time2 = parseInstant("2021-08-01T12:00:00 ET")

timeDiff = diffNanos(time1, time2)
println timeDiff
```

### How instants print

Printing an `Instant`, for example with `println now()`, produces an ISO-8601 string in UTC, such as `2021-09-09T15:58:41.041Z`. The format consists of the following fields:

`yyyy-MM-ddTHH:mm:ss[.fff]Z`

- `yyyy`: the year
- `MM`: the month
- `dd`: the day
- `T`: the separator between the date and time
- `HH`: the hour of the day, from 00 to 23
- `mm`: the minute of the hour
- `ss`: the second of the minute
- `[.fff]`: the optional fraction of a second, printed with 3, 6, or 9 digits as needed, or omitted when it is zero
- `Z`: the time zone indicator for UTC

Instants always print in UTC. To show a value in a specific time zone, use [`formatDateTime`](../reference/time/datetime/formatDateTime.md).

## Time zones and aliases

Deephaven uses Java's [`ZoneId`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZoneId.html) class to store time zone information. In addition to standard zone IDs such as `UTC` or `Z` for Coordinated Universal Time, Deephaven supports time zone aliases such as `ET` for US Eastern time. The [`timeZone`](../reference/time/datetime/timeZone.md) function returns the `ZoneId` for a zone ID or alias:

```groovy order=:log
println timeZone("ET")
println timeZone("PT")
println timeZone("UTC")
```

By default, Deephaven uses the JVM's default time zone, which is UTC in the Deephaven Docker images. For example, [`writeCsv`](../reference/data-import-export/CSV/writeCsv.md) writes [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) columns in this time zone unless you pass it a different one. To change the default, see [How to set the timezone](../reference/community-questions/set-timezone.md).

The following example parses the same clock time in the `ET` (US Eastern) and `MT` (US Mountain) time zones. Both values print in UTC, ending with `Z`, so the output shows the two-hour difference between the time zones.

```groovy order=:log
time1 = parseInstant("2021-01-01T01:00 ET")
time2 = parseInstant("2021-01-01T01:00 MT")

println time1
println time2
```

## Related documentation

- [How to use filters](../how-to-guides/use-filters.md)
- [How to work with strings](../how-to-guides/work-with-strings.md)
- [date-time](../reference/query-language/types/date-time.md)
- [Date-time literals in query strings](../how-to-guides/date-time-literals.md)
- [Business calendars](../how-to-guides/business-calendar.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
