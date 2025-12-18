---
title: Work with time
sidebar_label: Work with time
---

This guide discusses working with time in Deephaven. Deephaven is a real-time data platform that handles streaming data of various formats and sizes, offering a unified interface for the ingestion, manipulation, and analysis of this data. A significant part of the Deephaven experience involves working with data that signifies specific moments in time: calendar dates, time-periods or durations, time zones, and more. These can appear as data in Deephaven tables, literals in query strings, or values in Python scripts. So, it is important to learn about the best tools for these different jobs. Additionally, because the Deephaven query engine is implemented in Java and queries are often written in Python, there are some details to consider when crossing between the two languages. This guide will show you how to work with time efficiently in all cases.

> [!NOTE]
> This document provides references to Javadocs in numerous locations, as they are essential for understanding and working with time in Deephaven. For those new to Javadocs or in need of a refresher, refer to our [guide](../how-to-guides/read-javadocs.md) on how to read Javadocs.

Because time-based data types are integral to working with Deephaven, understanding their representations and how to manipulate them is critical. This will be true for both server-side applications and client-side applications. As such, the date-time types natively supported in Deephaven are a good starting point for this discussion.

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
| [`java.time.Period`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Period.html)               | A [`Period`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Period.html) is a date-based amount of time in the [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) system, such as "P2y3m4d" (2 years, 3 months and 4 days).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

## The Deephaven Query Language

Despite the fact that all of Deephaven's natively-supported types are Java types, the most common pattern for using Deephaven is through the Python API. So, how can these Java types be created or manipulated from Python? For this, Deephaven offers the Deephaven Query Language.

The Deephaven Query Language (DQL) is the primary way of expressing commands directly to the query engine. It is responsible for translating the user's intention into compiled code that the engine can execute. DQL is written with strings - called query strings - that can contain a mixture of Java and Python code. Thus, DQL query strings are the entry point to a universe of powerful built-in tools and Python-Java interoperability. These query strings are often used in the context of table operations, like creating new columns or applying filters. Here's a simple DQL example:

```python test-set=1
from deephaven import empty_table

t = (
    empty_table(10)
    .update(
        [
            "Col1 = ii",
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

In this example, the query engine converts these simple query strings to Java, compiles them, and executes them. When query strings include Python functions, the query engine uses both Java and Python to evaluate the expressions.

> [!NOTE]
> To learn more about the details of the DQL syntax and exactly what these commands do, refer to the DQL section in our User Guide, which includes guides on [writing formulas](../how-to-guides/formulas.md), [working with strings](../how-to-guides/work-with-strings.md), [using built-in functions](../how-to-guides/built-in-functions.md), and more.

There are four important tools provided by DQL that are relevant to the discussion on date-time types.

### 1. Built-in Java functions

Deephaven has a collection of [built-in functions](../reference/query-language/query-library/auto-imported-functions.md) that are useful for working with date-time types. For the sake of performance, these functions are implemented in Java. DQL supports calling these functions directly in query strings, opening up all of Deephaven's [built-in Java functions](../reference/query-language/query-library/auto-imported-functions.md) to the Python interface. The following example uses the built-in Deephaven function [`now`](<https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#now()>) to get the current system time as a Java [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html):

```python test-set=2
from deephaven import empty_table

t = empty_table(5).update("CurrentTime = now()")
```

> [!NOTE]
> [`now`](<https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#now()>) uses the [current clock](<https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#currentClock()>) of the Deephaven engine. This clock is typically the system clock, but it may be set to a simulated clock when replaying tables.

These [functions](../reference/query-language/query-library/auto-imported-functions.md) can also be applied to columns, constants, and variables. This slightly more complex example uses the built-in Deephaven function [`epochDaysToLocalDate`](<https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochDaysToLocalDate(long)>) to create a [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html) from a long that represents the number of days since the [Unix epoch](https://en.wikipedia.org/wiki/Unix_time):

```python test-set=3
from deephaven import empty_table

t = empty_table(5).update(
    ["DaysSinceEpoch = ii", "LocalDateColumn = epochDaysToLocalDate(DaysSinceEpoch)"]
)
```

In addition to functions, Deephaven offers many [built-in constants](/core/javadoc/io/deephaven/time/DateTimeUtils.html) to simplify expressions involving time values. These constants, like [`SECOND`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#SECOND), [`DAY`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#DAY), and [`YEAR_365`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#YEAR_365), are equal to the number of nanoseconds in a given time period. Many of Deephaven's built-in functions operate at nanosecond resolution, so these constants provide a simple way to work with nanoseconds:

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

Java is an [object-oriented programming language](https://docs.oracle.com/javase/tutorial/java/concepts/). As such, all Java objects have associated methods. These methods can be called in query strings. Here is an example that builds upon the previous example, and uses the [`getDayOfWeek`](<https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/LocalDate.html#getDayOfWeek()>) method bound to each [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html) object to extract the day of the week for each [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html):

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
- `LocalDateColumn` is a Java [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html) object.
- [`epochDaysToLocalDate`](<https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#epochDaysToLocalDate(long)>) is a Java function from the [built-in Deephaven library](../reference/query-language/query-library/auto-imported-functions.md).
- `DayOfWeek` is the return value of [`getDayOfWeek`](<https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/LocalDate.html#getDayOfWeek()>), a Java method bound to the [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html) class.

DQL enables them all to be used seamlessly from the Python API!

### 3. Arithmetic and inequality operators

Query strings support syntactic sugar for special operators such as `+`, `-`, `>`, `<`, `>=`, etc. for Java time types! For instance, it makes sense to add a [`Period`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Period.html) to an [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html), or a [`Duration`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Duration.html) to an [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html), or to multiply a [`Duration`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Duration.html) by an integer. This example uses the built-in [`parsePeriod`](<https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parsePeriod(java.lang.String)>) and [`parseDuration`](<https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseDuration(java.lang.String)>) functions to create period and duration columns from strings. Then, the overloaded addition operator `+` is used to add them to the `Timestamp` column, and the overloaded multiplication operator `*` is used to create a column with timestamps that increment daily:

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

### 4. Date-times using DQL

In Deephaven, date-time values can be expressed using very simple [literal](<https://en.wikipedia.org/wiki/Literal_(computer_programming)>) syntax. These literal values can be used directly in query strings or as string inputs to [built-in functions](../reference/query-language/query-library/auto-imported-functions.md).

> [!TIP]
> In query strings, time literals are denoted with _single quotes_.

This example creates [`Duration`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Duration.html) columns from a time literal as well as from a string parsed by [`parseDuration`](<https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseDuration(java.lang.String)>):

```python test-set=6
from deephaven import empty_table

t = empty_table(5).update(
    ["DurationFromLiteral = 'PT24h'", "DurationFromString = parseDuration(`PT24h`)"]
)
```

> [!NOTE]
> Note the difference in single quotes for the time literal and back quotes for the string.

Using query string time literals can yield more compact and more efficient code. In the prior example, `` parseDuration(`PT24h`) `` is evaluated for every single row in the table, but here `'PT24h'` is only evaluated once for the entire table. This can lead to massive performance differences for large tables:

```python test-set=7, order=t1,t2 default=:log
from deephaven import empty_table
import time

t1_start = time.time()
t1 = empty_table(100000000).update("DurationColumn = parseDuration(`PT24h`)")
t1_end = time.time()

t2_start = time.time()
t2 = empty_table(100000000).update("DurationColumn = 'PT24h'")
t2_end = time.time()

t1_time = t1_end - t1_start
t2_time = t2_end - t2_start
print("Using built-in parse function: ", t1_time)
print("Using literal: ", t2_time)
```

Most of the seven key Java types can be created using literals or functions like [`parseDuration`](<https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseDuration(java.lang.String)>):

```python test-set=7 order=t1,t2,t3,t4,t5,t6,t7
from deephaven import empty_table

# ZoneId columns can be created with literals or the parseTimeZone built-in.
# The literal or string argument must be a valid Java time zone.
t1 = empty_table(1).update(
    ["TimeZone1 = 'GMT'", "TimeZone2 = parseTimeZone(`America/New_York`)"]
)

# LocalDate columns can be created with literals or the parseLocalDate built-in.
# The literal or string argument must use the ISO-8601 date format 'YYYY-MM-DD'.
t2 = empty_table(1).update(
    ["LocalDate1 = '2022-03-28'", "LocalDate2 = parseLocalDate(`2075-08-08`)"]
)

# LocalTime columns can be created with literals or the parseLocalTime built-in.
# The literal or string argument must use the ISO-8601 time format 'hh:mm:ss[.SSSSSSSSS]'.
t3 = empty_table(1).update(
    ["LocalTime1 = '10:15:30.46'", "LocalTime2 = parseLocalTime(`12:01:01.4567`)"]
)

# Instant columns can be created with literals or the parseInstant built-in.
# The literal or string arguments must use the ISO-8601 date-time format `yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ`.
t4 = empty_table(1).update(
    [
        "Instant1 = '2015-01-30T12:34:56Z'",
        "Instant2 = parseInstant(`2023-11-21T21:30:45Z`)",
    ]
)

# ZonedDateTime columns cannot be created with literals, as they are indistinguishable from Instant literals.
# ZonedDateTime columns can be created with the parseZonedDateTime built-in,
# or by localizing the time zone of an Instant with the atZone() method.
# The string arguments must use the ISO-8601 date-time format `yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ`.
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
# The literal or string arguments must use the ISO-8601 period format 'PnYnMnD'.
# Negative periods are represented as 'P-nY-nM-nD' or '-PnYnMnD'.
t7 = empty_table(1).update(
    ["Period1 = 'P2Y3M10D'", "Period2 = parsePeriod(`P10Y0M3D`)"]
)
```

### Put it all together

To illustrate the power and ease of working with natively supported date times, this example uses time literals, operator-overloaded arithmetic, and Java time methods together to create timestamps, compute time differences, and extract information about those timestamps in Tokyo and New York timezones:

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

Hopefully, it's apparent that working with date-times in Deephaven queries is a breeze. With all of the [built-in time functions](/core/javadoc/io/deephaven/time/DateTimeUtils.html), most date-time operations can be accomplished with native Deephaven operations.

## Date-times in Python

Like Java, Python has its own ecosystem of types for representing dates, times, and durations. However, _unlike_ Java, date-time types in Python are implemented and supported by Python _packages_, which can be thought of as extensions to the language that make it easy for users to share complex code at scale. Each package that implements its own date-time types also provides functions and methods for working with those types. This means that in Python, there are often many ways to solve a date-time problem.

Three primary Python packages support date-time types and operations: [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime), [`numpy`](https://numpy.org), and [`pandas`](https://pandas.pydata.org).

### The [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime) package

The [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime) package is a part of the Python Standard Library, so it comes with every Python installation. The package provides several types to represent various date-time quantities:

| Type                                                                              | Represents                                                                |
| --------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| [`date`](https://docs.python.org/3/library/datetime.html#datetime.date)           | Dates with no time or time zone information                               |
| [`time`](https://docs.python.org/3/library/datetime.html#datetime.time)           | Times with no date information, optionally includes time zone information |
| [`datetime`](https://docs.python.org/3/library/datetime.html#datetime.datetime)   | Date-times, optionally includes time zone information                     |
| [`timedelta`](https://docs.python.org/3/library/datetime.html#datetime.timedelta) | Durations or distances between specific points in time                    |
| [`tzinfo`](https://docs.python.org/3/library/datetime.html#datetime.tzinfo)       | Time zones, generally used in tandem with another date-time type          |

Many types in the [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime) package can be equipped with a time-zone. An instance of a type with a time zone is called _aware_, while instances of types with no time zone information are called _naive_.

The API is simple:

```python test-set=9
import datetime

some_date = datetime.datetime(2021, 11, 18, 12, 21, 36)
print(some_date)
```

It's easy to convert to different time zones:

```python test-set=9
cst_tz = datetime.timezone(offset=-datetime.timedelta(hours=5))
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

And they can be constructed directly:

```python test-set=9
another_duration = datetime.timedelta(days=13, hours=14, minutes=56)
print(another_duration)
```

The simplicity of the [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime) API makes it very attractive and easy to use. However, array-like operations are not natively supported. This means that loops or list comprehensions are often required to perform time-based operations on arrays of date-time objects, which can hamper performance considerably.

### The [`numpy`](https://numpy.org) package

[`numpy`](https://numpy.org) is the leading implementation of arrays and array operations in Python. It's not a part of the Standard Library. [`numpy`](https://numpy.org) supports many different array types, including date-time types. Only the essential date-time types are supported:

| Type                                                                                          | Represents                                                 |
| --------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| [`datetime64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.datetime64)   | Dates, times, and date-times with no time zone information |
| [`timedelta64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.timedelta64) | Durations or distances between specific points in time     |

Unlike the [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime) package, [`numpy`](https://numpy.org) _does not_ support any kind of time zone awareness. In the language of the [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime) package, all of the [`numpy`](https://numpy.org) types are naive. Time zone offsets can be manually added to any [`numpy`](https://numpy.org) date-time value, but that can get cumbersome.

```python test-set=10
import numpy as np

datetime1 = np.datetime64("now")
datetime2 = np.datetime64("2019-07-11T21:04:02")
print(datetime1, datetime2)
print(datetime2 - datetime1)
```

Since [`numpy`](https://numpy.org) is built around arrays, many of its methods for creating and manipulating arrays will work on date-time types:

```python test-set=10
datetime_array = np.arange(
    np.datetime64("2005-01-01"), np.datetime64("2006-01-01"), np.timedelta64(1, "h")
)
print(datetime_array[0:10])
```

The array-first model means that operations can be performed on all array elements at once:

```python test-set=10
print(datetime1 - datetime_array)
```

[`numpy`](https://numpy.org)'s performance is hard to argue with. However, the API's support for various common time-based operations is sparse, and the lack of time zone support means that [`numpy`](https://numpy.org) cannot always be the best tool for the job.

### The [`pandas`](https://pandas.pydata.org) package

[`pandas`](https://pandas.pydata.org) is a popular Python package among data scientists, machine learning experts, and more. It provides substantial date-time support, and offers the best of both [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime) and [`numpy`](https://numpy.org): time-zone awareness, a rich API, and array-like operations for maximum performance.

[`pandas`](https://pandas.pydata.org) uses different types to represent scalar and array versions of each type:

| Type                                                                                            | Represents                                                                          |
| ----------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| [`Timestamp`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html) | Singular dates, times, or date-times, optionally includes time zone information     |
| [`DatetimeIndex`](https://pandas.pydata.org/docs/reference/api/pandas.DatetimeIndex.html)       | Sequences of dates, times, or date-times, optionally includes time zone information |
| [`Timedelta`](https://pandas.pydata.org/docs/reference/api/pandas.Timedelta.html)               | Singular durations less than a day in length                                        |
| [`TimedeltaIndex`](https://pandas.pydata.org/docs/reference/api/pandas.TimedeltaIndex.html)     | Sequences of durations, each less than a day in length                              |
| [`Period`](https://pandas.pydata.org/docs/reference/api/pandas.Period.html)                     | Singular durations lasting a day or more                                            |
| [`PeriodIndex`](https://pandas.pydata.org/docs/reference/api/pandas.PeriodIndex.html)           | Sequences of durations, each lasting a day or more                                  |

The date-time types also support imposing a time-zone, but no explicit time zone type is provided, as in the [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime) package.

Creating date-times (and arrays of date-times) in pandas is easy:

```python test-set=11
import pandas as pd

datetime_start = pd.Timestamp("2021-05-30T18:30:30")
datetime_array = pd.DatetimeIndex(
    [datetime_start + pd.Timedelta(i, unit="day") for i in range(365)]
)
print(datetime_array[0:10])
```

The API supports methods - like [`tz_localize`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DatetimeIndex.tz_localize.html) - to impose a time zone on entire arrays:

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

## [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time)

The [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) Python library is a small set of functions that enable conversions between Python's common date-time types and their equivalent Java types. This provides a bridge between Python and Java that can be used with DQL. With these functions, the possible workflows are endless.

### Python to Java conversions

The [`to_j_*`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_duration) series of functions from [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) enable converting any of the common Python date-time types from [`datetime`](https://docs.python.org/3/library/datetime.html#module-datetime), [`numpy`](https://numpy.org), or [`pandas`](https://pandas.pydata.org) to one of the seven key Java types.

The [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) library includes:

- [`to_j_time_zone`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_time_zone)
- [`to_j_local_date`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_date)
- [`to_j_local_time`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_time)
- [`to_j_instant`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_instant)
- [`to_j_zdt`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_zdt)
- [`to_j_duration`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_duration)
- [`to_j_period`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_period)

Each of these functions convert Python date-time types to the respective Java analogs.

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

The metadata table reveals that the types are indeed the [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) types that are expected. In this way, anything that can be created in Python can be directly converted to Java values that the query engine can efficiently use.

This functionality may be used to extract date-time information from an existing dataset and use it in Deephaven queries programmatically:

```python test-set=13 order=crypto_df,crypto_dh,trimmed_crypto_dh
from deephaven import read_csv
import deephaven.time as dhtime
import numpy as np
import pandas as pd

# Load crypto data into a pandas dataframe and a Deephaven table
crypto_df = pd.read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/crypto_sept7.csv"
)
crypto_dh = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/crypto.csv"
)

# Raw data does not already have TZ information, so we include it manually
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

In addition to providing functions to convert Python types to their equivalent Java types, [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) provides a suite of functions to convert Java types to Python types. These functions support converting any of the seven key Java types to their Python equivalents.

The Java-to-Python conversion methods included in [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) are:

- [`to_date`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_date)
- [`to_time`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_time)
- [`to_datetime`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_datetime)
- [`to_timedelta`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_timedelta)
- [`to_np_datetime64`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_datetime64)
- [`to_np_timedelta64`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_timedelta64)
- [`to_pd_timestamp`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timestamp)
- [`to_pd_timedelta`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timedelta)

A common use case for this functionality is in writing Python functions that act on Deephaven columns from within query strings. Here's an example that trains a simple [ARIMA model](https://en.wikipedia.org/wiki/Autoregressive_integrated_moving_average) on a toy dataset and uses a Python function to make forecasts with that model in a Deephaven table:

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
    return prediction


t_testing_eval = t_testing.update(["YPred = predict(Timestamp)"])
```

Evaluating the [mean squared error](https://en.wikipedia.org/wiki/Mean_squared_error) of this model shows that it performs well on average:

```python test-set=14
import deephaven.agg as agg

mse = t_testing_eval.update("SqErr = Math.pow(Y - YPred, 2)").agg_by(
    agg.avg("MeanSqErr = SqErr")
)
```

### Current time and the Deephaven Clock

The [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) module includes two functions, [`dh_now`](/core/pydoc/code/deephaven.time.html#deephaven.time.dh_now) and [`dh_today`](/core/pydoc/code/deephaven.time.html#deephaven.time.dh_now) that return current date-time information according to the _Deephaven clock_.

Typically, the Deephaven clock is set to the system clock of the computer the software is running on. However, this is not always the case. In particular, [TableReplayer](../reference/table-operations/create/Replayer.md) sets the Deephaven clock to a simulated clock that aligns with the timestamps being replayed. Thus, the Deephaven clock will be different from the system clock, and it is important to be aware of cases where this may arise.

Here is simple example of [`dh_now`](/core/pydoc/code/deephaven.time.html#deephaven.time.dh_now) and [`dh_today`](/core/pydoc/code/deephaven.time.html#deephaven.time.dh_now):

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

The Deephaven engine defaults to [Coordinated Universal Time (UTC)](https://en.wikipedia.org/wiki/Coordinated_Universal_Time), but [this can be changed](../reference/community-questions/set-timezone.md) if needed. Additionally, the [time zone displayed by the UI can be changed](../how-to-guides/set-date-time-format.md) without modifying the underlying engine time zone.

Deephaven can make use of any valid [Java time zone](https://docs.oracle.com/middleware/12211/wcs/tag-ref/MISC/TimeZones.html) within queries. However, full Java time zone names like `America/Chicago` can be cumbersome to type out repeatedly. To make query strings shorter, Deephaven supports the use of [common time zone aliases](https://github.com/deephaven/deephaven-core/blob/main/props/configs/src/main/resources/default_time_zone_aliases.csv) that can be used in place of the full Java names:

```python test-set=16
from deephaven import empty_table

t = empty_table(1).update(
    [
        "ChicagoTime = '2017-04-29T10:30:00 America/Chicago'",
        "ChicagoTimeAlias = '2017-04-29T10:30:00 CT'",
    ]
)
```

When Deephaven's default time zone aliases are not enough, the [`time_zone_alias_add`](/core/pydoc/code/deephaven.time.html#deephaven.time.time_zone_alias_add) function from [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) can be used to create a custom alias for any time zone. This custom alias can then be removed with [`time_zone_alias_rm`](/core/pydoc/code/deephaven.time.html#deephaven.time.time_zone_alias_rm). Here's an example of creating an alias for [Indian Standard Time](https://en.wikipedia.org/wiki/Indian_Standard_Time), and then removing it:

```python test-set=16
from deephaven import empty_table
import deephaven.time as dhtime

dhtime.time_zone_alias_add("IST", "Asia/Calcutta")
t = empty_table(1).update(
    [
        "IndiaTime = '2017-04-29T10:30:00 IST'",
    ]
)

dhtime.time_zone_alias_rm("IST")
```

> [!NOTE]
> Deephaven only supports using one alias per time zone. To create a custom alias for a time zone that already has a default alias, the default must first be removed with [`time_zone_alias_rm`](/core/pydoc/code/deephaven.time.html#deephaven.time.time_zone_alias_rm) before the new alias is added with [`time_zone_alias_add`](/core/pydoc/code/deephaven.time.html#deephaven.time.time_zone_alias_add).

### Parse and format date-time columns

Date-time values in real datasets often do not come formatted in a way that Deephaven can automatically convert to one of the key Java types. Take this example:

```python test-set=17
from deephaven import read_csv

gsod = read_csv("/data/examples/GSOD/csv/station_data.csv")
```

The `BEGIN` and `END` columns are dates, formatted as `YYYYMMDD`. However, Deephaven does not recognize that format as a date-time format, and interprets the columns as integers:

```python test-set=17
gsod_meta = gsod.meta_table
```

This means that none of the date-time functionality previously discussed will be available. To remedy this, [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) provides the [`simple_date_format`](/core/pydoc/code/deephaven.time.html#deephaven.time.simple_date_format) function, which makes parsing ingested date-time values easy.

To use [`simple_date_format`](/core/pydoc/code/deephaven.time.html#deephaven.time.simple_date_format), pass a string that describes the format of the _input_ date-time values. Since the `BEGIN` and `END` columns are formatted as `YYYYMMDD`, the argument to [`simple_date_format`](/core/pydoc/code/deephaven.time.html#deephaven.time.simple_date_format) should be `"YYYYMMDD"`:

```python test-set=17
from deephaven.time import simple_date_format

# pass the format of the input date-time
input_format = simple_date_format("YYYYMMDD")
input_format2 = simple_date_format("YYYYMMDD")
```

Now, `input_format` is an object that can be used in a query to format the `BEGIN` and `END` columns. This is done with the [`parse`](<https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/SimpleDateFormat.html#parse(java.lang.String,java.text.ParsePosition)>) method, which accepts the input date-time columns as strings. So, the `int` values must be converted to strings before calling [`parse`](<https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/SimpleDateFormat.html#parse(java.lang.String,java.text.ParsePosition)>). Finally, call [`toInstant`](<https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Date.html#toInstant()>) on the result to get the formatted date-time as a Java [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html):

```python test-set=17
# use String.valueOf() to get int as string, then pass to input_format.parse(), finally call toInstant()
format_gsod = gsod.update(
    formulas=[
        "BEGIN = input_format.parse(String.valueOf(BEGIN)).toInstant()",
        "END = input_format2.parse(String.valueOf(END)).toInstant()",
    ]
)
```

The resulting columns can then be converted to any of the desired Java date-time types using Deephaven's built-in functions. This example converts `BEGIN` and `END` to [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html) columns using [`toLocalDate`](<https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#toLocalDate(java.time.Instant,java.time.ZoneId)>):

```python test-set=17
format_gsod_local_date = format_gsod.update(
    formulas=["BEGIN = toLocalDate(BEGIN, 'UTC')", "END = toLocalDate(END, 'UTC')"]
)
```

To verify that these columns have the correct type, check the metadata table:

```python test-set=17
format_gsod_meta = format_gsod_local_date.meta_table
```

Once the data have been converted to a proper Java date-time format, they can be reformatted using [`format_columns`](../reference/table-operations/format/format-columns.md). Note that this reformatting does not affect the internal representation of the data, but rather how it's presented in the UI. Only [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) columns can be formatted, so other types must be converted to an [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) before reformatting them. The desired format must be a [valid Java date-time format](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/format/DateTimeFormatter.html).

Here's an example that converts the [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html) columns from above to [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) columns, and reformats them using [`format_columns`](../reference/table-operations/format/format-columns.md):

```python test-set=17
reformat_gsod_local_date = format_gsod_local_date.update(
    [
        "BEGIN = toInstant(BEGIN, '00:00:00', 'UTC')",
        "END = toInstant(END, '00:00:00', 'UTC')",
    ]
).format_columns(["BEGIN = Date(`EEE MMM d, y`)", "END = Date(`EEE MMM d, y`)"])
```

## Performance and the Python-Java Boundary

Because Deephaven queries can contain Python and the Deephaven query engine is implemented in Java, performance-conscious users should be aware of [Python-Java boundary crossings](../conceptual/python-java-boundary.md). Every time a query's execution passes between Python and Java, there is a performance penalty. Fast queries must be written to minimize such [boundary crossings](../conceptual/python-java-boundary.md).

> [!WARNING]
> Avoid [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) functions in query strings. Because [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) provides functions that convert between Python and Java types, every call crosses the [Python-Java boundary](../conceptual/python-java-boundary.md). Thus, each call incurs a tiny overhead. This overhead can add up when applied millions or billions of times during a query. Instead, use the [built-in functions](../reference/query-language/query-library/auto-imported-functions.md) that provide the same functionality and do not cross the [Python-Java boundary](../conceptual/python-java-boundary.md).

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

Thus, in total, there are `3n` boundary crossings, which significantly slows the query.

Now let us consider a case where a sequence of time values is accessed by index in a query. In the following example, this computation is done three different ways. Each way has a different number of boundary crossings and therefore different performance. Comparing the execution times, there is a significant difference. The small overheads from the boundary crossings can add up.

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


# Case 1: 2n boundary crossings -- 2 Python function calls per row
def get1(idx):
    return timestamps[idx]


start = time.time()
t1 = t_empty.update("Timestamp = (Instant)dhtime.to_j_instant(get1(i))")
print(
    f"Case 1: {time.time() - start} sec;  {(time.time() - start) / t_empty.size} sec/row"
)


# Case 2: n boundary crossings -- 1 Python function call per row
def get2(idx):
    return dhtime.to_j_instant(timestamps[idx])


start = time.time()
t2 = t_empty.update("Timestamp = (Instant)get2(i)")
print(
    f"Case 2: {time.time() - start} sec;  {(time.time() - start) / t_empty.size} sec/row"
)

# Case 3: 1 boundary crossing -- create a Java array of Java instants
start = time.time()
j_array = dht.array(dht.Instant, timestamps)
t3 = t_empty.update("Timestamp = j_array[i]")
print(
    f"Case 3: {time.time() - start} sec;  {(time.time() - start) / t_empty.size} sec/row"
)
```

Finally, let us use Python to get the start of the current day and then use that [`datetime`](https://docs.python.org/3/library/datetime.html#datetime.datetime) in a query. In this example, the first case does the time manipulation mostly in Python. The second case converts the Python [`datetime`](https://docs.python.org/3/library/datetime.html#datetime.datetime) to a Java [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) one time for use in the query.

```python test-set=18 order=t1,t2 default=:log
import time
import datetime
from deephaven import empty_table
import deephaven.time as dht

t_empty = empty_table(1_000_000)
start_of_day = datetime.datetime.combine(datetime.datetime.now(), datetime.time.min)


# Case 1: Work in terms of Python times
def plus_seconds(i):
    return dht.to_j_instant(start_of_day + datetime.timedelta(seconds=i))


start = time.time()
t1 = t_empty.update("Timestamp = (Instant) plus_seconds(ii)")
print(
    f"Case 1: {time.time() - start} sec;  {(time.time() - start) / t_empty.size} sec/row"
)

# Case 2: Make a single conversion from a Python time to a Java time and then work in terms of Java times
start_of_day_instant = dht.to_j_instant(start_of_day)
start = time.time()
t2 = t_empty.update("Timestamp = start_of_day_instant + ii*SECOND")
print(
    f"Case 2: {time.time() - start} sec;  {(time.time() - start) / t_empty.size} sec/row"
)
```

Again, the resulting tables are identical, but the execution times are quite different, and boundary crossings still tell the story. Use [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) methods outside of query strings for the fastest performance.

## Appendix

| Python Type                                                                                            | Java Type                                                                                                     | Java to Python                                                                               | Python to Java                                                                           |
| ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| [`datetime.date`](https://docs.python.org/3/library/datetime.html#datetime.date)                       | [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html)         | [`to_date`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_date)                     | [`to_j_local_date`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_date) |
| [`datetime.time`](https://docs.python.org/3/library/datetime.html#datetime.time)                       | [`LocalTime`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalTime.html)         | [`to_time`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_time)                     | [`to_j_local_time`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_time) |
| [`datetime.datetime`](https://docs.python.org/3/library/datetime.html#datetime.datetime)               | [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html)              | [`to_datetime`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_datetime)             | [`to_j_instant`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_instant)       |
| [`datetime.datetime`](https://docs.python.org/3/library/datetime.html#datetime.datetime)               | [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/ZonedDateTime.html) | [`to_datetime`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_datetime)             | [`to_j_zdt`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_zdt)               |
| [`datetime.timedelta`](https://docs.python.org/3/library/datetime.html#datetime.timedelta)             | [`Duration`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Duration.html)           | [`to_timedelta`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_timedelta)           | [`to_j_duration`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_duration)     |
| [`datetime.timedelta`](https://docs.python.org/3/library/datetime.html#datetime.timedelta)             | [`Period`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Period.html)               | [`to_timedelta`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_timedelta)           | [`to_j_period`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_period)         |
| [`datetime.tzinfo`](https://docs.python.org/3/library/datetime.html#datetime.tzinfo)                   | [`ZoneId`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/ZoneId.html)               | NA                                                                                           | [`to_j_time_zone`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_time_zone)   |
| [`numpy.datetime64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.datetime64)      | [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html)         | [`to_np_datetime64`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_datetime64)   | [`to_j_local_date`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_date) |
| [`numpy.datetime64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.datetime64)      | [`LocalTime`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalTime.html)         | [`to_np_datetime64`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_datetime64)   | [`to_j_local_time`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_time) |
| [`numpy.datetime64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.datetime64)      | [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html)              | [`to_np_datetime64`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_datetime64)   | [`to_j_instant`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_instant)       |
| [`numpy.datetime64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.datetime64)      | [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/ZonedDateTime.html) | [`to_np_datetime64`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_datetime64)   | [`to_j_zdt`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_zdt)               |
| [`numpy.timedelta64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.timedelta64)    | [`Duration`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Duration.html)           | [`to_np_timedelta64`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_timedelta64) | [`to_j_duration`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_duration)     |
| [`numpy.timedelta64`](https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.timedelta64)    | [`Period`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Period.html)               | [`to_np_timedelta64`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_np_timedelta64) | [`to_j_period`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_period)         |
| [`pandas.Timestamp`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html) | [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html)         | [`to_pd_timestamp`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timestamp)     | [`to_j_local_date`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_date) |
| [`pandas.Timestamp`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html) | [`LocalTime`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalTime.html)         | [`to_pd_timestamp`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timestamp)     | [`to_j_local_time`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_local_time) |
| [`pandas.Timestamp`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html) | [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html)              | [`to_pd_timestamp`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timestamp)     | [`to_j_instant`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_instant)       |
| [`pandas.Timestamp`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html) | [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/ZonedDateTime.html) | [`to_pd_timestamp`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timestamp)     | [`to_j_zdt`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_zdt)               |
| [`pandas.DatetimeIndex`](https://pandas.pydata.org/docs/reference/api/pandas.DatetimeIndex.html)       | NA                                                                                                            | NA                                                                                           | NA                                                                                       |
| [`pandas.Timedelta`](https://pandas.pydata.org/docs/reference/api/pandas.Timedelta.html)               | [`Duration`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Duration.html)           | [`to_pd_timedelta`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timedelta)     | [`to_j_duration`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_duration)     |
| [`pandas.Timedelta`](https://pandas.pydata.org/docs/reference/api/pandas.Timedelta.html)               | [`Period`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Period.html)               | [`to_pd_timedelta`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_pd_timedelta)     | [`to_j_period`](/core/pydoc/code/deephaven.time.html#deephaven.time.to_j_period)         |
| [`pandas.TimedeltaIndex`](https://pandas.pydata.org/docs/reference/api/pandas.TimedeltaIndex.html)     | NA                                                                                                            | NA                                                                                           | NA                                                                                       |
| [`pandas.Period`](https://pandas.pydata.org/docs/reference/api/pandas.Period.html)                     | NA                                                                                                            | NA                                                                                           | NA                                                                                       |
| [`pandas.PeriodIndex`](https://pandas.pydata.org/docs/reference/api/pandas.PeriodIndex.html)           | NA                                                                                                            | NA                                                                                           | NA                                                                                       |

## Related documentation

- [`deephaven.time` Pydoc](/core/pydoc/code/deephaven.time.html)
- [Date-time literals in query strings](../how-to-guides/date-time-literals.md)
- [Business calendars](../how-to-guides/business-calendar.md)
- [The Python-Java boundary](python-java-boundary.md)
