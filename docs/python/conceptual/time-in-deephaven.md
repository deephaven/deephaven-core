---
title: Work with time
---

This guide shows how to work with dates, times, durations, periods, and time zones in Deephaven. Deephaven is a real-time data platform that handles streaming data of various formats and sizes, with a unified interface for ingesting, manipulating, and analyzing it. Much of that data marks specific moments in time, so date-times are central to working in Deephaven. These values appear as data in tables, as literals in query strings, and as values in Python scripts.

This guide covers:

- The Java date-time types the engine supports natively.
- The query-language tools for working with those types.
- The common Python date-time packages.
- The [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) module, which reads the Deephaven clock, manages time zones, creates formatters for parsing date-time strings in table columns, and converts between Python and Java types.
- How to keep those conversions from slowing queries down.

> [!NOTE]
> This guide links to Javadocs throughout. If you're new to Javadocs, see [Javadocs for Python users](../how-to-guides/read-javadocs.md).

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

All of Deephaven's natively supported types are Java types, but most people use Deephaven through the Python API. The Deephaven Query Language lets Python code create and manipulate these Java types.

The Deephaven Query Language (DQL) is the primary way of expressing commands directly to the query engine. It is responsible for translating the user's intention into compiled code that the engine can execute. You write DQL as strings, called query strings, that can contain a mixture of Java and Python code. Query strings give you access to Deephaven's built-in functions and let Python and Java code work together. You typically use query strings in table operations, such as creating columns or filtering rows. Here's a simple DQL example:

```python test-set=1
from deephaven import empty_table

t = (
    empty_table(10)
    .update(
        [
            "Col1 = i",
            "Col2 = Col1 + 3",
            "Col3 = 2 * Math.sin(Col1 + Col2)",
        ]
    )
    .where(
        [
            "Col1 % 2 == 0",
            "Col3 > 0",
        ]
    )
)
```

In this example, the query engine converts these simple query strings to Java, compiles them, and executes them. When query strings include Python functions, the query engine uses both Java and Python to evaluate the expressions. `i` and `ii` are [special variables](../reference/query-language/variables/special-variables.md) that hold each row's index, as an `int` and a `long` respectively.

> [!NOTE]
> To learn more about the details of the DQL syntax and exactly what these commands do, refer to the DQL section in our User Guide, which includes guides on [writing formulas](../how-to-guides/formulas.md), [working with strings](../how-to-guides/work-with-strings.md), [using built-in functions](../how-to-guides/built-in-functions.md), and more.

DQL provides four tools for working with date-time types.

### 1. Built-in Java functions

Deephaven has a collection of [built-in functions](../reference/query-language/query-library/auto-imported/index.md) that are useful for working with date-time types. For the sake of performance, these functions are implemented in Java. DQL supports calling these functions directly in query strings, which makes all of Deephaven's built-in Java functions available from the Python API. The following example uses the built-in Deephaven function [`now`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#now()) to get the current time as a Java [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html):

```python test-set=2
from deephaven import empty_table

t = empty_table(5).update("CurrentTime = now()")
```

> [!NOTE]
> The [`now`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#now()) function uses the [current clock](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#currentClock()) of the Deephaven engine. To learn when that clock differs from the system clock, see [Current time and the Deephaven clock](#current-time-and-the-deephaven-clock).

You can also apply these [functions](../reference/query-language/query-library/auto-imported/index.md) to columns, constants, and variables. This slightly more complex example uses the built-in Deephaven function [`epochDaysToLocalDate`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochDaysToLocalDate(long)) to create a [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html) from a `long` that represents the number of days since the [Unix epoch](https://en.wikipedia.org/wiki/Unix_time):

```python test-set=3
from deephaven import empty_table

t = empty_table(5).update(
    ["DaysSinceEpoch = ii", "LocalDateColumn = epochDaysToLocalDate(DaysSinceEpoch)"]
)
```

In addition to functions, Deephaven offers many [built-in constants](/core/javadoc/io/deephaven/time/DateTimeUtils.html) for expressions involving time values. Many built-in functions work in nanoseconds, and these constants convert between nanoseconds and larger units:

- [`MICRO`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#MICRO), [`MILLI`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#MILLI), [`SECOND`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#SECOND), [`MINUTE`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#MINUTE), [`HOUR`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#HOUR), [`DAY`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#DAY), [`WEEK`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#WEEK), and [`YEAR_365`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#YEAR_365) each equal the number of nanoseconds in that unit.
- [`YEAR_AVG`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#YEAR_AVG) equals the number of nanoseconds in an average Gregorian year.
- Reciprocal constants such as [`SECONDS_PER_NANO`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#SECONDS_PER_NANO) convert nanoseconds to larger units.

The following example divides a nanosecond count by several of these constants:

```python test-set=3
from deephaven import empty_table

t = empty_table(1).update(
    [
        "CurrentTime = now()",
        "NanosSinceEpoch = epochNanos(CurrentTime)",
        "SecondsSinceEpoch = NanosSinceEpoch / SECOND",
        "MinutesSinceEpoch = NanosSinceEpoch / MINUTE",
        "DaysSinceEpoch = NanosSinceEpoch / DAY",
        "YearsSinceEpoch = NanosSinceEpoch / YEAR_AVG",
    ]
)
```

### 2. Java object methods

Java is an [object-oriented programming language](https://docs.oracle.com/javase/tutorial/java/concepts/), so every Java object has methods. You can call those methods in query strings. This example extends the [`epochDaysToLocalDate`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochDaysToLocalDate(long)) example from [Built-in Java functions](#1-built-in-java-functions), which turns a count of days since the Unix epoch into a [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html). It uses the [`getDayOfWeek`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html#getDayOfWeek()) method of each `LocalDate` object to extract the day of the week:

```python test-set=4
from deephaven import empty_table

t = empty_table(5).update(
    [
        "DaysSinceEpoch = ii",
        "LocalDateColumn = epochDaysToLocalDate(DaysSinceEpoch)",
        "DayOfWeek = LocalDateColumn.getDayOfWeek()",
    ]
)
```

To be clear:

- `DaysSinceEpoch` is a 64-bit integer.
- `LocalDateColumn` is a Java [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html) object.
- [`epochDaysToLocalDate`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochDaysToLocalDate(long)) is a Java function from the [built-in Deephaven library](../reference/query-language/query-library/auto-imported/index.md).
- `DayOfWeek` is the return value of [`getDayOfWeek`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html#getDayOfWeek()), a Java method bound to the [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html) class.

DQL lets you call both the built-in functions and the methods of Java objects from the Python API.

### 3. Arithmetic and comparison operators

Query strings support arithmetic operators such as `+`, `-`, and `*`, and comparison operators such as `<`, `<=`, `>`, `>=`, and `==`, for Java time types. For instance, you can add a [`Period`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html) or a [`Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html) to an [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html), or multiply a `Duration` or a `Period` by an integer. This example uses the built-in [`parsePeriod`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parsePeriod(java.lang.String)) and [`parseDuration`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseDuration(java.lang.String)) functions to create period and duration columns from strings. The overloaded `+` operator then adds them to the `Timestamp` column, and the overloaded `*` operator creates a column of timestamps that increment daily:

```python test-set=5
from deephaven import empty_table

t = empty_table(5).update(
    [
        "Timestamp = now()",
        "PeriodColumn = parsePeriod(`P1D`)",
        "DurationColumn = parseDuration(`PT24h`)",
        "TimePlusPeriod = Timestamp + PeriodColumn",
        "TimePlusDuration = Timestamp + DurationColumn",
        "IncreasingTime = Timestamp + PeriodColumn * i",
    ]
)
```

> [!NOTE]
> This example uses backticks to represent strings. For more info, see the guide on [working with strings](../how-to-guides/work-with-strings.md).

An [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) has no time zone. You can add a [`Period`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html) to an `Instant` only when the period is in days or weeks. Each of those days counts as exactly 24 hours, as it does in a [`Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html). To add months or years, or days that follow daylight-saving changes, add the period to a [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZonedDateTime.html) instead.

### 4. Date-time literals

You can write date-time values as [literals](https://en.wikipedia.org/wiki/Literal_(computer_programming)) directly in query strings. The same text, written as a backtick string, can be passed to [built-in](../reference/query-language/query-library/auto-imported/index.md) parse functions such as `parseDuration`.

> [!TIP]
> In query strings, time literals are denoted with _single quotes_, and strings with backticks.

This example creates [`Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html) columns from a time literal as well as from a string parsed by [`parseDuration`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseDuration(java.lang.String)):

```python test-set=6
from deephaven import empty_table

t = empty_table(5).update(
    ["DurationFromLiteral = 'PT24h'", "DurationFromString = parseDuration(`PT24h`)"]
)
```

Using query string time literals can yield more compact and more efficient code. In the prior example, the engine evaluates ``parseDuration(`PT24h`)`` once per row but evaluates the literal `'PT24h'` once for the whole table. On large tables, the literal is much faster:

```python test-set=7 order=t1,t2 default=:log
from deephaven import empty_table
import time

t1_start = time.time()
t1 = empty_table(10_000_000).update("DurationColumn = parseDuration(`PT24h`)")
t1_end = time.time()

t2_start = time.time()
t2 = empty_table(10_000_000).update("DurationColumn = 'PT24h'")
t2_end = time.time()

t1_time = t1_end - t1_start
t2_time = t2_end - t2_start
print("Using built-in parse function: ", t1_time)
print("Using literal: ", t2_time)
```

You can create most of the seven key Java types with literals or with functions like [`parseDuration`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseDuration(java.lang.String)):

```python test-set=7 order=t1,t2,t3,t4,t5,t6,t7
from deephaven import empty_table

# ZoneId columns can be created with literals or the parseTimeZone built-in.
# The literal or string argument must be a valid Java time zone.
t1 = empty_table(1).update(
    ["TimeZone1 = 'GMT'", "TimeZone2 = parseTimeZone(`America/New_York`)"]
)

# LocalDate columns can be created with literals or the parseLocalDate built-in.
# The literal or string argument must use the ISO-8601 date format 'yyyy-MM-dd'.
t2 = empty_table(1).update(
    ["LocalDate1 = '2022-03-28'", "LocalDate2 = parseLocalDate(`2075-08-08`)"]
)

# LocalTime columns can be created with literals or the parseLocalTime built-in.
# The literal or string argument must use the ISO-8601 time format 'HH:mm[:ss[.SSSSSSSSS]]'.
t3 = empty_table(1).update(
    ["LocalTime1 = '10:15:30.46'", "LocalTime2 = parseLocalTime(`12:01:01.4567`)"]
)

# Instant columns can be created with literals or the parseInstant built-in.
# The literal or string arguments must include a time zone, as in `yyyy-MM-ddTHH:mm[:ss[.SSSSSSSSS]] TZ` or with a trailing `Z` for UTC.
t4 = empty_table(1).update(
    [
        "Instant1 = '2015-01-30T12:34:56Z'",
        "Instant2 = parseInstant(`2023-11-21T21:30:45Z`)",
    ]
)

# ZonedDateTime columns cannot be created with literals, as they are indistinguishable from Instant literals.
# ZonedDateTime columns can be created with the parseZonedDateTime built-in,
# or by localizing the time zone of an Instant with the atZone() method.
# The string arguments must use the ISO-8601 date-time format `yyyy-MM-ddTHH:mm[:ss[.SSSSSSSSS]] TZ`.
t5 = empty_table(1).update(
    [
        "ZonedDateTime1 = parseZonedDateTime(`2021-11-03T01:02:03 GMT`)",
        "ZonedDateTime2 = '2023-11-21T21:30:45 GMT'.atZone('GMT')",
    ]
)

# Duration columns can be created with literals or the parseDuration built-in.
# The literal or string arguments must use the ISO-8601 duration format 'PnDTnHnMn.nS'.
# Negative durations are represented as 'P-nDT-nH-nM-n.nS' or '-PnDTnHnMn.nS'.
t6 = empty_table(1).update(
    ["Duration1 = 'PT6H30M30S'", "Duration2 = parseDuration(`PT10H`)"]
)

# Period columns can be created with literals or the parsePeriod built-in.
# The literal or string arguments must use the ISO-8601 period format 'PnYnMnWnD', omitting any unused unit.
# Negative periods are represented as 'P-nY-nM-nD' or '-PnYnMnD'.
t7 = empty_table(1).update(
    ["Period1 = 'P2Y3M10D'", "Period2 = parsePeriod(`P10Y0M3D`)"]
)
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

```python test-set=8 order=t,t_meta
from deephaven import empty_table

# Create reference time and timestamp column using operator overloading and Java method multipliedBy()
t = empty_table(100).update(
    [
        "Reference = '2025-10-15T13:23 ET'",
        "Timestamp = Reference + 'PT1h'.multipliedBy(ii)",
    ]
)

# Use operator overloading and diffMinutes() built-in to get time since reference time
t = t.update(
    [
        "DifferenceNanos = Timestamp - Reference",
        "DifferenceMin = diffMinutes(Reference, Timestamp)",
    ]
)

# Finally, use built-in functions and time zone literals to get date and
# day of week in two different time zones
t = t.update(
    [
        "DateTokyo = toLocalDate(Timestamp, 'Asia/Tokyo')",
        "DayTokyo = dayOfWeek(Timestamp, 'Asia/Tokyo')",
        "DateNY = toLocalDate(Timestamp, 'America/New_York')",
        "DayNY = dayOfWeek(Timestamp, 'America/New_York')",
    ]
)

# Assess metadata to see that types are as expected
t_meta = t.meta_table
```

Hopefully, it's apparent that working with date-times in Deephaven queries is a breeze. The [built-in time functions](/core/javadoc/io/deephaven/time/DateTimeUtils.html) cover most date-time operations in query strings.

## Date-times in Python

Like Java, Python has its own ecosystem of types for representing dates, times, and durations. Python's standard library has its own date-time types in the `datetime` package, but unlike Java, where libraries share the `java.time` types, popular Python packages such as `numpy` and `pandas` define their own date-time types. Each package that implements its own date-time types also provides functions and methods for working with those types. This means that in Python, there are often many ways to solve a date-time problem.

Three primary Python packages support date-time types and operations: [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime), [`numpy`](https://numpy.org), and [`pandas`](https://pandas.pydata.org).

### The `datetime` package

The [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime) package is a part of the Python Standard Library, so it comes with every Python installation. The package provides several types to represent various date-time quantities:

| Type                                                                              | Represents                                                                |
| --------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| [`date`](https://docs.python.org/3/library/datetime.html#datetime.date)           | Dates with no time or time zone information                               |
| [`time`](https://docs.python.org/3/library/datetime.html#datetime.time)           | Times with no date information, optionally includes time zone information |
| [`datetime`](https://docs.python.org/3/library/datetime.html#datetime.datetime)   | Date-times, optionally includes time zone information                     |
| [`timedelta`](https://docs.python.org/3/library/datetime.html#datetime.timedelta) | Durations or distances between specific points in time                    |
| [`tzinfo`](https://docs.python.org/3/library/datetime.html#datetime.tzinfo)       | Time zones, generally used in tandem with another date-time type          |

Many types in the [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime) package can carry a time zone. An instance of a type with a time zone is called _aware_, while instances of types with no time zone information are called _naive_.

The API is simple:

```python test-set=9
import datetime

some_date = datetime.datetime(2021, 11, 18, 12, 21, 36)
print(some_date)
```

It's easy to convert to different time zones:

```python test-set=9
cst_tz = datetime.timezone(offset=-datetime.timedelta(hours=6))
some_date_cst = some_date.astimezone(cst_tz)
print(some_date_cst)
```

Durations result from date-time arithmetic:

```python test-set=9
date1 = datetime.datetime.now().date()
date2 = datetime.date(2011, 3, 9)
duration = date2 - date1
print(duration)
```

You can also construct durations directly:

```python test-set=9
another_duration = datetime.timedelta(days=13, hours=14, minutes=56)
print(another_duration)
```

The [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime) API is simple to use. However, `datetime` has no array operations, so you often need loops or list comprehensions to operate on arrays of date-time objects, which can slow code considerably.

### The `numpy` package

[`numpy`](https://numpy.org) is the leading implementation of arrays and array operations in Python. It's not a part of the Standard Library. `numpy` supports many different array types, including date-time types. It has only two date-time types:

| Type                                                                                          | Represents                                                 |
| --------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| [`datetime64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.datetime64)   | Dates, times, and date-times with no time zone information |
| [`timedelta64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.timedelta64) | Durations or distances between specific points in time     |

Unlike the [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime) package, [`numpy`](https://numpy.org) _does not_ support any kind of time zone awareness. In the language of the `datetime` package, all of the `numpy` types are naive. You can add time zone offsets to any `numpy` date-time value manually, but that gets cumbersome.

```python test-set=10
import numpy as np

datetime1 = np.datetime64("now")
datetime2 = np.datetime64("2019-07-11T21:04:02")
print(datetime1, datetime2)
print(datetime2 - datetime1)
```

Since [`numpy`](https://numpy.org) is built around arrays, many of its methods for creating and manipulating arrays work on date-time types:

```python test-set=10
datetime_array = np.arange(
    np.datetime64("2005-01-01"), np.datetime64("2006-01-01"), np.timedelta64(1, "h")
)
print(datetime_array[0:10])
```

The array-first model lets you operate on all array elements at once:

```python test-set=10
print(datetime1 - datetime_array)
```

[`numpy`](https://numpy.org) array operations are fast. However, its API covers few common time-based operations, and it has no time zone support.

### The `pandas` package

[`pandas`](https://pandas.pydata.org) is a popular Python package among data scientists, machine learning experts, and more. It provides substantial date-time support and combines the strengths of [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime) and [`numpy`](https://numpy.org): time zone awareness, a rich API, and fast array operations.

[`pandas`](https://pandas.pydata.org) uses different types to represent scalar and array versions of each type:

| Type                                                                                        | Represents                                                                                  |
| ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| [`Timestamp`](https://pandas.pydata.org/docs/reference/api/pandas.Timestamp.html)           | Singular dates, times, or date-times, optionally includes time zone information             |
| [`DatetimeIndex`](https://pandas.pydata.org/docs/reference/api/pandas.DatetimeIndex.html)   | Sequences of dates, times, or date-times, optionally includes time zone information         |
| [`Timedelta`](https://pandas.pydata.org/docs/reference/api/pandas.Timedelta.html)           | Singular durations, including durations longer than one day                                 |
| [`TimedeltaIndex`](https://pandas.pydata.org/docs/reference/api/pandas.TimedeltaIndex.html) | Sequences of durations                                                                      |
| [`Period`](https://pandas.pydata.org/docs/reference/api/pandas.Period.html)                 | Singular spans of time with a fixed frequency (for example, a specific day, month, or year) |
| [`PeriodIndex`](https://pandas.pydata.org/docs/reference/api/pandas.PeriodIndex.html)       | Sequences of such spans                                                                     |

The date-time types also support imposing a time zone, but unlike the [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime) package, [`pandas`](https://pandas.pydata.org) provides no explicit time zone type.

In [`pandas`](https://pandas.pydata.org), you can create single date-times and arrays of date-times:

```python test-set=11
import pandas as pd

datetime_start = pd.Timestamp("2021-05-30T18:30:30")
datetime_array = pd.DatetimeIndex(
    [datetime_start + pd.Timedelta(i, unit="day") for i in range(365)]
)
print(datetime_array[0:10])
```

Methods such as [`tz_localize`](https://pandas.pydata.org/docs/reference/api/pandas.DatetimeIndex.tz_localize.html) impose a time zone on an entire array:

```python test-set=11
datetime_array_cst_tz = datetime_array.tz_localize("America/Chicago")
print(datetime_array_cst_tz[0:10])
```

And there are helper attributes and methods for many common date-time operations, all with array support:

```python test-set=11
print(datetime_array_cst_tz[0:10].day_of_week)
print(datetime_array_cst_tz[0:10].time)
print(datetime_array_cst_tz[0:10].month)
```

Overall, [`pandas`](https://pandas.pydata.org) offers the richest API with array support. It's more complex than [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime) or [`numpy`](https://numpy.org), but can solve a wider range of problems than both of the alternatives.

## `deephaven.time`

The [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) Python module reads the Deephaven clock, manages time zone aliases, creates Java date formatters for parsing date-time strings, and converts between Python's common date-time types and their Java equivalents. Use its conversion functions to pass Python date-time values into DQL query strings and to bring Java date-time values back into Python.

### Current time and the Deephaven clock

The [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) module includes two functions, [`dh_now`](/core/pydoc/code/deephaven.time.html#deephaven.time.dh_now) and [`dh_today`](/core/pydoc/code/deephaven.time.html#deephaven.time.dh_today), that return the current date and time according to the _Deephaven clock_.

By default, the Deephaven clock is the system clock of the computer running Deephaven. [`DateTimeUtils.setClock`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#setClock(io.deephaven.base.clock.Clock)) replaces it with a custom clock, such as a simulated clock. When that happens, [`dh_now`](/core/pydoc/code/deephaven.time.html#deephaven.time.dh_now) and [`dh_today`](/core/pydoc/code/deephaven.time.html#deephaven.time.dh_today) return times from that clock instead of the system clock. [Replaying historical data](../how-to-guides/replay-data.md) does not change the Deephaven clock: the replayer drives its replayed tables from its own simulated clock.

Here is a simple example of [`dh_now`](/core/pydoc/code/deephaven.time.html#deephaven.time.dh_now) and [`dh_today`](/core/pydoc/code/deephaven.time.html#deephaven.time.dh_today):

```python test-set=15
import deephaven.time as dhtime
import datetime

print(datetime.datetime.now())
print(dhtime.dh_now())
print(dhtime.dh_today())
```

### Time zones and aliases

Keeping track of time zones is an important task when working with date-time data. [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) offers three functions to facilitate working with time zones in Deephaven:

- [`dh_time_zone`](/core/pydoc/code/deephaven.time.html#deephaven.time.dh_time_zone)
- [`time_zone_alias_add`](/core/pydoc/code/deephaven.time.html#deephaven.time.time_zone_alias_add)
- [`time_zone_alias_rm`](/core/pydoc/code/deephaven.time.html#deephaven.time.time_zone_alias_rm)

To get the current time zone of the running Deephaven engine, use [`dh_time_zone`](/core/pydoc/code/deephaven.time.html#deephaven.time.dh_time_zone):

```python test-set=16
import deephaven.time as dhtime

print(dhtime.dh_time_zone())
```

The Deephaven engine uses the JVM's default time zone, which is [Coordinated Universal Time (UTC)](https://en.wikipedia.org/wiki/Coordinated_Universal_Time) in the Deephaven Docker images. To change it, see [How to set the timezone](../reference/community-questions/set-timezone.md). To change the time zone the UI displays without changing the engine time zone, see [How to set date-time format](../how-to-guides/set-date-time-format.md).

Deephaven can make use of any valid [Java time zone](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZoneId.html) within queries. However, full Java time zone names like `America/Chicago` can be cumbersome to type out repeatedly. To make query strings shorter, Deephaven supports [common time zone aliases](https://github.com/deephaven/deephaven-core/blob/main/props/configs/src/main/resources/default_time_zone_aliases.csv) that you can use in place of the full Java names:

```python test-set=16
from deephaven import empty_table

t = empty_table(1).update(
    [
        "ChicagoTime = '2017-04-29T10:30:00 America/Chicago'",
        "ChicagoTimeAlias = '2017-04-29T10:30:00 CT'",
    ]
)
```

When the default aliases aren't enough, use [`time_zone_alias_add`](/core/pydoc/code/deephaven.time.html#deephaven.time.time_zone_alias_add) to create a custom alias for any time zone, and [`time_zone_alias_rm`](/core/pydoc/code/deephaven.time.html#deephaven.time.time_zone_alias_rm) to remove it. This example creates an alias for [Indian Standard Time](https://en.wikipedia.org/wiki/Indian_Standard_Time) and then removes it:

```python test-set=16
from deephaven import empty_table
import deephaven.time as dhtime

dhtime.time_zone_alias_add("IST", "Asia/Kolkata")
t = empty_table(1).update(
    [
        "IndiaTime = '2017-04-29T10:30:00 IST'",
    ]
)

dhtime.time_zone_alias_rm("IST")
```

> [!NOTE]
> Deephaven supports one alias per time zone. To give a time zone with a default alias a custom one, remove the default with [`time_zone_alias_rm`](/core/pydoc/code/deephaven.time.html#deephaven.time.time_zone_alias_rm), then add the new alias with [`time_zone_alias_add`](/core/pydoc/code/deephaven.time.html#deephaven.time.time_zone_alias_add).

### Parse and format date-time columns

Date-time values in real datasets often use formats that [`read_csv`](../reference/data-import-export/CSV/readCsv.md) doesn't recognize as date-times. Take this example:

```python test-set=17
from deephaven import read_csv

gsod = read_csv("/data/examples/GSOD/csv/station_data.csv")
```

The `BEGIN` and `END` columns are dates, formatted as `yyyyMMdd`. However, [`read_csv`](../reference/data-import-export/CSV/readCsv.md) doesn't recognize that format as a date-time format, so it reads the columns as integers:

```python test-set=17
gsod_meta = gsod.meta_table
```

As integers, these columns can't use Deephaven's [built-in date-time functions](../reference/query-language/query-library/auto-imported/index.md). To parse them, use the [`simple_date_format`](/core/pydoc/code/deephaven.time.html#deephaven.time.simple_date_format) function from [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time).

To use [`simple_date_format`](/core/pydoc/code/deephaven.time.html#deephaven.time.simple_date_format), pass a string that describes the format of the _input_ date-time values. The `BEGIN` and `END` columns use the format `yyyyMMdd`, so pass `"yyyyMMdd"`.

Use lowercase `y` and `d` in the pattern. In Java date-time patterns, uppercase `Y` means week-year and uppercase `D` means day-of-year.

> [!WARNING]
> A [`SimpleDateFormat`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/SimpleDateFormat.html) is not thread-safe, and the engine can compute columns concurrently and split one column across threads. Giving each column its own formatter, as the `simple_date_format` examples below do, is enough for small tables. For large tables, parse with a thread-safe [`java.time.format.DateTimeFormatter`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/format/DateTimeFormatter.html) instead.

For example, this query parses `BEGIN` and `END` straight to [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html) with a thread-safe formatter:

```python test-set=17
import jpy

basic_date = jpy.get_type("java.time.format.DateTimeFormatter").BASIC_ISO_DATE

gsod_dates = gsod.update(
    [
        "BEGIN = LocalDate.parse(String.valueOf(BEGIN), basic_date)",
        "END = LocalDate.parse(String.valueOf(END), basic_date)",
    ]
)
```

For a small table, create a separate `simple_date_format` formatter for each column:

```python test-set=17
from deephaven.time import simple_date_format

# pass the format of the input date-time
input_format = simple_date_format("yyyyMMdd")
input_format2 = simple_date_format("yyyyMMdd")
```

`input_format` and `input_format2` parse the `BEGIN` and `END` columns in a query. Their [`parse`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/DateFormat.html#parse(java.lang.String)) method takes a string, so convert each `int` value to a string first. Then call [`toInstant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Date.html#toInstant()) on the result to get a Java [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html):

```python test-set=17
# use String.valueOf() to get int as string, then pass to input_format.parse(), finally call toInstant()
format_gsod = gsod.update(
    formulas=[
        "BEGIN = input_format.parse(String.valueOf(BEGIN)).toInstant()",
        "END = input_format2.parse(String.valueOf(END)).toInstant()",
    ]
)
```

Deephaven's built-in functions convert the resulting columns to other Java date-time types. This example converts `BEGIN` and `END` to [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html) columns using [`toLocalDate`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#toLocalDate(java.time.Instant,java.time.ZoneId)):

```python test-set=17
format_gsod_local_date = format_gsod.update(
    formulas=[
        "BEGIN = toLocalDate(BEGIN, timeZone())",
        "END = toLocalDate(END, timeZone())",
    ]
)
```

To verify that these columns have the correct type, check the metadata table:

```python test-set=17
format_gsod_meta = format_gsod_local_date.meta_table
```

After the columns hold a Java date-time type, use [`format_columns`](../reference/table-operations/format/format-columns.md) to change how the UI displays them. Formatting changes only the display, not the stored values. A `Date(...)` format in `format_columns` applies only to [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) and [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZonedDateTime.html) columns, so convert other types, such as [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html), to one of those first. The format string must be a [valid Java date-time format](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/format/DateTimeFormatter.html).

Here's an example that converts the [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html) columns from above to [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) columns, and reformats them using [`format_columns`](../reference/table-operations/format/format-columns.md):

```python test-set=17
reformat_gsod_local_date = format_gsod_local_date.update(
    [
        "BEGIN = toInstant(BEGIN, '00:00:00', 'UTC')",
        "END = toInstant(END, '00:00:00', 'UTC')",
    ]
).format_columns(["BEGIN = Date(`EEE MMM d, y`)", "END = Date(`EEE MMM d, y`)"])
```

### Python to Java conversions

The `to_j_*` functions in [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) convert scalar date-time values from [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime), [`numpy`](https://numpy.org), and [`pandas`](https://pandas.pydata.org) to the seven key Java types. They don't accept array types such as [`pandas.DatetimeIndex`](https://pandas.pydata.org/docs/reference/api/pandas.DatetimeIndex.html) and [`pandas.TimedeltaIndex`](https://pandas.pydata.org/docs/reference/api/pandas.TimedeltaIndex.html). They also don't accept [`pandas.Period`](https://pandas.pydata.org/docs/reference/api/pandas.Period.html) or [`pandas.PeriodIndex`](https://pandas.pydata.org/docs/reference/api/pandas.PeriodIndex.html). The [conversion summary](#conversion-summary) maps each Python type to its Java equivalent.

The Python-to-Java conversion functions in [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) are:

- [`to_j_time_zone`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_time_zone)
- [`to_j_local_date`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_date)
- [`to_j_local_time`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_time)
- [`to_j_instant`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_instant)
- [`to_j_zdt`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_zdt)
- [`to_j_duration`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_duration)
- [`to_j_period`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_period)

Here's an example using [`to_j_instant`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_instant):

```python test-set=12 order=t,t_meta
from deephaven import empty_table
import deephaven.time as dhtime
import datetime
import numpy as np
import pandas as pd

instant1 = dhtime.to_j_instant(datetime.datetime(2022, 7, 17, 12, 34, 56))
instant2 = dhtime.to_j_instant(np.datetime64("2022-07-17T12:34:56"))
instant3 = dhtime.to_j_instant(pd.Timestamp("2022-07-17T12:34:56"))

t = empty_table(1).update(
    ["Instant1 = instant1", "Instant2 = instant2", "Instant3 = instant3"]
)
t_meta = t.meta_table
```

The metadata table shows that all three columns are [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) columns. [`to_j_instant`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_instant) converts values from [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime), [`numpy`](https://numpy.org), and [`pandas`](https://pandas.pydata.org) to a Java type that the query engine uses directly.

The following example converts the earliest and latest timestamps in a [`pandas`](https://pandas.pydata.org) DataFrame to Java and uses them to filter a Deephaven table:

```python test-set=13 order=crypto_df,crypto_dh,trimmed_crypto_dh
from deephaven import read_csv
import deephaven.time as dhtime
import pandas as pd

# Load crypto data into a pandas dataframe and a Deephaven table
crypto_df = pd.read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/crypto_sept7.csv"
)
crypto_dh = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/crypto.csv"
)

# The raw values end in " Etc/UTC", which pandas can't parse, so drop the zone name and localize to UTC
crypto_df["dateTime"] = pd.DatetimeIndex(
    crypto_df["dateTime"].str.split(" ").str[0]
).tz_localize("UTC")

# Get Python timestamp values from the dataframe
min_timestamp = crypto_df["dateTime"].min()
max_timestamp = crypto_df["dateTime"].max()

# Convert the Python timestamps to Java
j_min = dhtime.to_j_instant(min_timestamp)
j_max = dhtime.to_j_instant(max_timestamp)

# Filter the Deephaven table using the min and max timestamps
trimmed_crypto_dh = crypto_dh.where("dateTime >= j_min && dateTime <= j_max").sort(
    "dateTime"
)
```

### Java to Python conversions

In addition to providing functions to convert Python types to their equivalent Java types, [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) provides a suite of functions to convert Java types to Python types. These functions convert the Java types [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html), [`LocalTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalTime.html), [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html), [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZonedDateTime.html), [`Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html), and [`Period`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html) to their Python equivalents. No function converts a Java [`ZoneId`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZoneId.html) to Python. The [conversion summary](#conversion-summary) lists the functions for the most common pairs of types.

The Java-to-Python conversion functions in [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) are:

- [`to_date`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_date)
- [`to_time`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_time)
- [`to_datetime`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_datetime)
- [`to_timedelta`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_timedelta)
- [`to_np_datetime64`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_datetime64)
- [`to_np_timedelta64`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_timedelta64)
- [`to_pd_timestamp`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timestamp)
- [`to_pd_timedelta`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timedelta)

Not every Java [`Period`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html) converts. [`to_timedelta`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_timedelta) and [`to_pd_timedelta`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timedelta) accept only day- or week-based periods, and [`to_np_timedelta64`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_timedelta64) accepts a period in a single unit of days, months, or years.

These functions are useful when a Python function called from a query string needs a Python date-time value, for example to pass it to a Python library that has no Java equivalent. Each such call crosses the Python-Java boundary at least once per row, plus once more for each conversion inside the function, so reserve this pattern for work that has no built-in Java function. See [Performance and the Python-Java boundary](#performance-and-the-python-java-boundary).

Here's an example that trains a simple [ARIMA model](https://en.wikipedia.org/wiki/Autoregressive_integrated_moving_average) on a toy dataset and uses a Python function to make forecasts with that model in a Deephaven table:

```python test-set=14 order=t_testing_eval,t_testing,t
import os

os.system("pip install statsmodels")
from statsmodels.tsa.arima.model import ARIMA

from deephaven import empty_table
import deephaven.pandas as dhpd
import deephaven.time as dhtime

t = empty_table(60 * 24).update(
    [
        "X = ii",
        "Timestamp = '2021-01-01T00:00:00Z' + 'PT1m' * X",
        "Y = 20 + X / 3 + Math.sin(X) + randomGaussian(0.0, 1.0)",
    ]
)

# train on first 18 hours, test on last 6
t_training = t.head(60 * 18)
t_testing = t.tail(60 * 6)

# ARIMA requires that the timestamp column be set as the index
training_df = dhpd.to_pandas(t_training).set_index("Timestamp")
model = ARIMA(
    endog=training_df["Y"].astype(float), order=(5, 1, 1), dates=training_df.index
)
res = model.fit()
print(res.summary())


# prediction function to apply to each timestamp.
# type hints enable Deephaven to interpret return type correctly
def predict(timestamp) -> float:
    # here, we use deephaven.time to convert the value from DH to something that the model can use
    pd_timestamp = dhtime.to_pd_timestamp(timestamp).tz_localize("UTC")
    prediction = res.predict(start=pd_timestamp)
    return float(prediction.iloc[0])


t_testing_eval = t_testing.update(["YPred = predict(Timestamp)"])
```

The following code evaluates the [mean squared error](https://en.wikipedia.org/wiki/Mean_squared_error) of the model's predictions on the test data:

```python test-set=14
import deephaven.agg as agg

mse = t_testing_eval.update("SqErr = Math.pow(Y - YPred, 2)").agg_by(
    agg.avg("MeanSqErr = SqErr")
)
```

### Conversion summary

The following table lists each Python date-time type, its Java equivalent, and the [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) functions that convert between them. NA means that no conversion function exists.

| Python type                                                                                         | Java type                                                                                                    | Java to Python                                                                               | Python to Java                                                                           |
| --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| [`datetime.date`](https://docs.python.org/3/library/datetime.html#datetime.date)                    | [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html)         | [`to_date`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_date)                     | [`to_j_local_date`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_date) |
| [`datetime.time`](https://docs.python.org/3/library/datetime.html#datetime.time)                    | [`LocalTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalTime.html)         | [`to_time`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_time)                     | [`to_j_local_time`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_time) |
| [`datetime.datetime`](https://docs.python.org/3/library/datetime.html#datetime.datetime)            | [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html)             | [`to_datetime`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_datetime)             | [`to_j_instant`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_instant)       |
| [`datetime.datetime`](https://docs.python.org/3/library/datetime.html#datetime.datetime)            | [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZonedDateTime.html) | [`to_datetime`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_datetime)             | [`to_j_zdt`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_zdt)               |
| [`datetime.timedelta`](https://docs.python.org/3/library/datetime.html#datetime.timedelta)          | [`Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html)           | [`to_timedelta`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_timedelta)           | [`to_j_duration`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_duration)     |
| [`datetime.timedelta`](https://docs.python.org/3/library/datetime.html#datetime.timedelta)          | [`Period`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html)               | [`to_timedelta`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_timedelta)           | [`to_j_period`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_period)         |
| [`datetime.tzinfo`](https://docs.python.org/3/library/datetime.html#datetime.tzinfo)                | [`ZoneId`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZoneId.html)               | NA                                                                                           | [`to_j_time_zone`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_time_zone)   |
| [`numpy.datetime64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.datetime64)   | [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html)         | NA                                                                                           | [`to_j_local_date`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_date) |
| [`numpy.datetime64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.datetime64)   | [`LocalTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalTime.html)         | NA                                                                                           | [`to_j_local_time`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_time) |
| [`numpy.datetime64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.datetime64)   | [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html)             | [`to_np_datetime64`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_datetime64)   | [`to_j_instant`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_instant)       |
| [`numpy.datetime64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.datetime64)   | [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZonedDateTime.html) | [`to_np_datetime64`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_datetime64)   | [`to_j_zdt`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_zdt)               |
| [`numpy.timedelta64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.timedelta64) | [`Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html)           | [`to_np_timedelta64`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_timedelta64) | [`to_j_duration`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_duration)     |
| [`numpy.timedelta64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.timedelta64) | [`Period`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html)               | [`to_np_timedelta64`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_timedelta64) | [`to_j_period`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_period)         |
| [`pandas.Timestamp`](https://pandas.pydata.org/docs/reference/api/pandas.Timestamp.html)            | [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html)         | NA                                                                                           | [`to_j_local_date`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_date) |
| [`pandas.Timestamp`](https://pandas.pydata.org/docs/reference/api/pandas.Timestamp.html)            | [`LocalTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalTime.html)         | NA                                                                                           | [`to_j_local_time`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_time) |
| [`pandas.Timestamp`](https://pandas.pydata.org/docs/reference/api/pandas.Timestamp.html)            | [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html)             | [`to_pd_timestamp`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timestamp)     | [`to_j_instant`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_instant)       |
| [`pandas.Timestamp`](https://pandas.pydata.org/docs/reference/api/pandas.Timestamp.html)            | [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZonedDateTime.html) | [`to_pd_timestamp`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timestamp)     | [`to_j_zdt`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_zdt)               |
| [`pandas.DatetimeIndex`](https://pandas.pydata.org/docs/reference/api/pandas.DatetimeIndex.html)    | NA                                                                                                           | NA                                                                                           | NA                                                                                       |
| [`pandas.Timedelta`](https://pandas.pydata.org/docs/reference/api/pandas.Timedelta.html)            | [`Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html)           | [`to_pd_timedelta`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timedelta)     | [`to_j_duration`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_duration)     |
| [`pandas.Timedelta`](https://pandas.pydata.org/docs/reference/api/pandas.Timedelta.html)            | [`Period`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Period.html)               | [`to_pd_timedelta`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timedelta)     | [`to_j_period`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_period)         |
| [`pandas.TimedeltaIndex`](https://pandas.pydata.org/docs/reference/api/pandas.TimedeltaIndex.html)  | NA                                                                                                           | NA                                                                                           | NA                                                                                       |
| [`pandas.Period`](https://pandas.pydata.org/docs/reference/api/pandas.Period.html)                  | NA                                                                                                           | NA                                                                                           | NA                                                                                       |
| [`pandas.PeriodIndex`](https://pandas.pydata.org/docs/reference/api/pandas.PeriodIndex.html)        | NA                                                                                                           | NA                                                                                           | NA                                                                                       |

## Performance and the Python-Java boundary

Because Deephaven queries can contain Python and the Deephaven query engine is implemented in Java, performance-conscious users should be aware of [Python-Java boundary crossings](../conceptual/python-java-boundary.md). Every time a query's execution passes between Python and Java, there is a performance penalty. To keep queries fast, minimize boundary crossings.

> [!WARNING]
> Avoid [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) functions in query strings. Because `deephaven.time` provides functions that convert between Python and Java types, every call crosses the Python-Java boundary. Thus, each call incurs a tiny overhead. This overhead can add up when applied millions or billions of times during a query. Instead, use the [built-in functions](../reference/query-language/query-library/auto-imported/index.md) that provide the same functionality and do not cross the Python-Java boundary.

Here is an example that demonstrates the effects that excess boundary crossings can have on performance:

```python test-set=18 order=t1,t2
from deephaven import empty_table
import deephaven.time as dhtime
import datetime
import time

# 30 days worth of data, one entry every second
t = empty_table(60 * 60 * 24 * 30).update(
    "Timestamp = '2022-01-01T00:00:00 America/New_York' + SECOND * ii"
)


# many boundary crossings -- timestamp is passed in as a Java instant
def shift_five_seconds(timestamp):
    # convert a Java Instant (timestamp) to a Python datetime
    shifted_datetime = dhtime.to_datetime(timestamp) + datetime.timedelta(seconds=5)
    # convert a Python datetime to a Java Instant
    return dhtime.to_j_instant(shifted_datetime)


t1_start = time.time()
t1 = t.update("ShiftedTimestamp = (Instant) shift_five_seconds(Timestamp)")
t1_end = time.time()

# no boundary crossings -- pure Java query string
t2_start = time.time()
t2 = t.update("ShiftedTimestamp = Timestamp + 'PT5s'")
t2_end = time.time()
```

The resulting tables are identical, but their execution times differ enormously:

```python test-set=18
t1_time = t1_end - t1_start
t2_time = t2_end - t2_start
print("Many boundary crossings: ", t1_time)
print("No boundary crossings: ", t2_time)
```

Careful inspection of the slower code reveals many boundary crossings. If the table has `n` rows, there are:

1. `n` boundary crossings to call the Python function.
2. `n` boundary crossings to convert the Java [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) to a Python [`datetime`](https://docs.python.org/3/library/datetime.html#datetime.datetime).
3. `n` boundary crossings to convert a Python [`datetime`](https://docs.python.org/3/library/datetime.html#datetime.datetime) to a Java [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html).

Thus, in total, there are `3n` boundary crossings, which significantly slows the query. To use a Python date-time value in a query, convert it to Java once, outside the query string, and then use Java arithmetic in the query string. The `j_min` and `j_max` values in [Python to Java conversions](#python-to-java-conversions) follow this pattern.

The next example looks up time values by index in a query in three ways. Each way crosses the boundary a different number of times, and the execution times show how those small per-crossing overheads add up.

```python test-set=18 order=t1,t2,t3 default=:log
from deephaven import empty_table
import deephaven.time as dhtime
import deephaven.dtypes as dht
import pandas as pd
import time

timestamps = pd.date_range(
    start="2023-01-01T00:00:00", end="2023-12-31T23:59:59", freq="min"
)
t_empty = empty_table(60 * 24 * 365)


# Case 1: 3n boundary crossings -- 2 Python function calls per row, plus a Java call inside to_j_instant
def get1(idx):
    return timestamps[idx]


start = time.time()
t1 = t_empty.update("Timestamp = (Instant)dhtime.to_j_instant(get1(i))")
print(
    f"Case 1: {time.time() - start} sec;  {(time.time() - start) / t_empty.size} sec/row"
)


# Case 2: 2n boundary crossings -- 1 Python function call per row, plus a Java call inside to_j_instant
def get2(idx):
    return dhtime.to_j_instant(timestamps[idx])


start = time.time()
t2 = t_empty.update("Timestamp = (Instant)get2(i)")
print(
    f"Case 2: {time.time() - start} sec;  {(time.time() - start) / t_empty.size} sec/row"
)

# Case 3: a few boundary crossings, independent of n -- create a Java array of Java instants once
start = time.time()
j_array = dht.array(dht.Instant, timestamps)
t3 = t_empty.update("Timestamp = j_array[i]")
print(
    f"Case 3: {time.time() - start} sec;  {(time.time() - start) / t_empty.size} sec/row"
)
```

## Related documentation

- [How to use filters](../how-to-guides/use-filters.md)
- [How to work with strings](../how-to-guides/work-with-strings.md)
- [date-time](../reference/query-language/types/date-time.md)
- [Date-time literals in query strings](../how-to-guides/date-time-literals.md)
- [Business calendars](../how-to-guides/business-calendar.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
- [The Python-Java boundary](python-java-boundary.md)
- [`deephaven.time` Pydoc](/core/pydoc/code/deephaven.time.html)
