---
title: Time operations cheat sheet
---

Deephaven is a real-time data platform, and many Deephaven applications use time-stamped data in some way. This cheat sheet outlines the tools available for working with that data. The guide on [working with time in Deephaven](../../conceptual/time-in-deephaven.md) provides conceptual detail that this cheat sheet does not cover.

## Date-time literals

In query strings, a value of more than one character in _single quotes_ is a date-time literal. The engine parses it as an [instant](../query-language/types/date-time.md#instant), a date, a time, a time zone, a [duration](../query-language/types/durations.md), or a [period](../query-language/types/periods.md). [Date-time literals in query strings](../../how-to-guides/date-time-literals.md) lists the formats for each type. Short zone names such as `ET` and `CT` are Deephaven [time zone aliases](#time-zone-aliases). The following example creates one column of each kind. Its [`meta`](../table-operations/metadata/meta.md) table, `timeLiteralsTypes`, shows each column's type:

```groovy order=timeLiteralsTypes,timeLiterals
timeLiterals = emptyTable(1).update(
    "Instant = '2021-01-01T12:30:01.123 ET'",
    "Date = '2000-06-30'",
    "Time = '15:36:44.000'",
    "Timezone1 = 'CT'",
    "Timezone2 = 'Europe/London'",
    "Duration = 'PT36m15s'",
    "Period = 'P2y3m4d'"
)

timeLiteralsTypes = timeLiterals.meta()
```

> [!NOTE]
> Date-time literals use single quotes. Strings in query strings use _backticks_.

## Create tables with timestamps

### `newTable`

[`newTable`](../table-operations/create/newTable.md) and [`instantCol`](../table-operations/create/instantCol.md) create a table with an [`Instant`](../query-language/types/date-time.md#instant) column. This example builds the values with [`parseInstant`](../time/datetime/parseInstant.md):

```groovy
firstTime = parseInstant("2021-07-04T08:00:00 ET")
secondTime = parseInstant("2021-09-06T12:30:00 ET")
thirdTime = parseInstant("2021-12-25T21:15:00 ET")

result = newTable(
    instantCol("DateTimes", firstTime, secondTime, thirdTime)
)
```

### `emptyTable`

To create a timestamped table with the query language, use [`emptyTable`](../table-operations/create/emptyTable.md) and [`update`](../table-operations/select/update.md):

```groovy
tEmptyTable = emptyTable(10).update(
    "Timestamp = '2023-06-01T00:00:00 UTC' + 'PT24h'.multipliedBy(ii)"
)
```

### `timeTable`

[`timeTable`](../table-operations/create/timeTable.md) creates a [ticking table](../../how-to-guides/time-table.md), which adds a new row at a fixed interval. It takes the interval in any of these forms:

- A [duration](../query-language/types/durations.md) string, such as `PT1S`, `PT1H30M`, or `PT00:00:00.001`.
- A number of nanoseconds, such as `5 * SECOND`, which uses a [built-in constant](#built-in-constants).

An optional first argument sets the start time, as an [`Instant`](../query-language/types/date-time.md#instant) or as a date-time string in the [format `parseInstant` accepts](#parse-date-times). When the start time is in the past, the table immediately contains one row for each interval between the start time and the current time:

```groovy ticking-table order=null
// Start now and add a row every second
tTimeTable = timeTable("PT1S")

// Start at a fixed time and add a row every hour
tTimeTableStart = timeTable("2024-01-01T00:00:00 ET", "PT1H")
```

## Parse time-stamped data

Each table example in this section converts a raw table and also shows the [`meta`](../table-operations/metadata/meta.md) table of both the raw and the converted table, so the column types can be compared before and after the conversion.

### Parse date-times

[`parseInstant`](../time/datetime/parseInstant.md) converts strings in an [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601)-style format to the [`Instant`](../query-language/types/date-time.md#instant) date-time type. The format is `yyyy-MM-ddTHH:mm:ss[.SSSSSSSSS] TZ`, where `TZ` is a time zone name or alias:

```groovy test-set=1 order=t1,t1Raw,t1Meta,t1RawMeta
t1Raw = newTable(
    stringCol(
        "Timestamp",
        "2023-01-01T12:30:01.00 CT",
        "2023-01-01T12:30:02.00 CT",
        "2023-01-01T12:30:03.00 CT"
    )
)

t1 = t1Raw.update("NewTimestamp = parseInstant(Timestamp)")

t1RawMeta = t1Raw.meta()
t1Meta = t1.meta()
```

[`parseInstant`](../time/datetime/parseInstant.md) accepts only ISO 8601-style date-times and plain integer offsets from the Unix epoch, so it cannot parse custom layouts such as `01/01/2023 12:30:01 CT`. To parse them, use a Java [`SimpleDateFormat`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/SimpleDateFormat.html) that describes their format:

```groovy test-set=1 order=t2,t2Raw,t2Meta,t2RawMeta
import java.text.SimpleDateFormat

t2Raw = newTable(
    stringCol(
        "Timestamp",
        "01/01/2023 12:30:01 CT",
        "01/01/2023 12:30:02 CT",
        "01/01/2023 12:30:03 CT"
    )
)

// Describe the existing format to SimpleDateFormat, and it can perform the conversion
inputFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss z")

// Replace the time zone "CT" with "CST" because SimpleDateFormat.parse() does not understand "CT"
// withSerial() keeps the engine from using the shared inputFormat on several threads at once
t2 = t2Raw.update(
    [
        Selectable.parse(
            "NewTimestamp = inputFormat.parse(Timestamp.replaceAll(`CT`, `CST`)).toInstant()"
        ).withSerial()
    ]
)

t2RawMeta = t2Raw.meta()
t2Meta = t2.meta()
```

> [!WARNING]
> A `SimpleDateFormat` is not thread-safe. On a large table, the engine can run a [formula](../../how-to-guides/formulas.md) on several threads at once, and a shared `SimpleDateFormat` can then return wrong values. The example above prevents this. It passes the formula to [`Selectable.parse`](/core/javadoc/io/deephaven/api/Selectable.html#parse(java.lang.String)) and marks the column serial with [`withSerial`](../../conceptual/query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where), so the engine never runs the formula concurrently with itself. The later examples on this page that use [`SimpleDateFormat`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/SimpleDateFormat.html) need the same change on large tables.

In this example, the strings use a different layout and already carry the abbreviation `CST`. `SimpleDateFormat` recognizes `CST`, so the query does not call `replaceAll`:

```groovy test-set=1 order=t3,t3Raw,t3Meta,t3RawMeta
import java.text.SimpleDateFormat

t3Raw = newTable(
    stringCol("Timestamp", "20230101-12:30:01 CST", "20230101-12:30:02 CST", "20230101-12:30:03 CST")
)

inputFormat = new SimpleDateFormat("yyyyMMdd-HH:mm:ss z")

t3 = t3Raw.update("NewTimestamp = inputFormat.parse(Timestamp).toInstant()")

t3RawMeta = t3Raw.meta()
t3Meta = t3.meta()
```

Here, the dates and times are stored in separate columns. Extract the data from each column and use [`parseInstant`](../time/datetime/parseInstant.md) or [`toInstant`](../time/datetime/toInstant.md) to get the final result:

```groovy test-set=1 order=t4,t4Raw,t4Meta,t4RawMeta
t4Raw = newTable(
    stringCol("Date", "2023-01-01", "2023-01-01", "2023-01-01"),
    stringCol("Time", "12:30:01 CT", "12:30:02 CT", "12:30:03 CT")
)

t4 = t4Raw.update(
    // First concatenate strings, then use parseInstant()
    "NewTimestamp1 = parseInstant(Date + `T` + Time)",
    // Alternatively, parse each component with parseLocalDate, parseLocalTime, and parseTimeZone, then use toInstant
    "NewTimestamp2 = toInstant(parseLocalDate(Date), parseLocalTime(Time.split(` `)[0]), parseTimeZone(Time.split(` `)[1]))"
)

t4RawMeta = t4Raw.meta()
t4Meta = t4.meta()
```

Java methods can also build the arguments that [`toInstant`](../time/datetime/toInstant.md) takes. Here, [`LocalDate.of`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html#of(int,int,int)) creates a [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html), and the example passes it to [`toInstant`](../time/datetime/toInstant.md):

```groovy test-set=1 order=t5,t5Raw,t5Meta,t5RawMeta
t5Raw = newTable(
    intCol("Month", 1, 1, 1),
    intCol("Day", 1, 1, 1),
    intCol("Year", 2023, 2023, 2023),
    stringCol("Time", "12:30:01", "12:30:02", "12:30:03"),
    stringCol("Timezone", "CT", "CT", "CT")
)

t5 = t5Raw.update(
    "NewTimestamp = toInstant(LocalDate.of(Year, Month, Day), parseLocalTime(Time), parseTimeZone(Timezone))"
)

t5RawMeta = t5Raw.meta()
t5Meta = t5.meta()
```

Integers can hold an offset from the [Unix epoch](https://en.wikipedia.org/wiki/Unix_time). [`epochSecondsToInstant`](../time/datetime/epochSecondsToInstant.md) converts a count of seconds to an [`Instant`](../query-language/types/date-time.md#instant). [`epochAutoToInstant`](../time/datetime/epochAutoToInstant.md) infers whether the offset is in seconds, milliseconds, microseconds, or nanoseconds from its magnitude:

```groovy test-set=1 order=tEpoch,tEpochRaw,tEpochMeta,tEpochRawMeta
tEpochRaw = newTable(
    longCol("EpochSeconds", 1672597801L, 1672597802L, 1672597803L)
)

tEpoch = tEpochRaw.update(
    "NewTimestamp1 = epochSecondsToInstant(EpochSeconds)",
    "NewTimestamp2 = epochAutoToInstant(EpochSeconds)"
)

tEpochRawMeta = tEpochRaw.meta()
tEpochMeta = tEpoch.meta()
```

[`parseInstant`](../time/datetime/parseInstant.md) and the epoch conversion functions also work in Groovy code outside of query strings:

```groovy test-set=1 order=:log
dateTimeObject = parseInstant("2021-07-04T08:00:00 ET")
println dateTimeObject.getClass()

fromEpoch = epochAutoToInstant(1631045972)
println fromEpoch
```

### Parse dates

When a date column holds strings in ISO 8601 format, such as `2023-01-01`, [`parseLocalDate`](../time/datetime/parseLocalDate.md) converts the column to a `LocalDate`:

```groovy test-set=1 order=t6,t6Raw,t6Meta,t6RawMeta
t6Raw = newTable(stringCol("Date", "2023-01-01", "2023-01-02", "2023-01-03"))

t6 = t6Raw.update("NewDate = parseLocalDate(Date)")

t6RawMeta = t6Raw.meta()
t6Meta = t6.meta()
```

When the column uses a non-ISO format, create a [`SimpleDateFormat`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/SimpleDateFormat.html) that describes that format. Parsing gives an [`Instant`](../query-language/types/date-time.md#instant), not a date, so the examples convert it back:

- The `SimpleDateFormat` parses a string with no time zone in the [engine time zone](#time-zones).
- [`timeZone`](../time/datetime/timeZone.md) returns that time zone.
- [`toLocalDate`](../time/datetime/toLocalDate.md), given the parsed `Instant` and the result of [`timeZone`](../time/datetime/timeZone.md), returns the original date.

```groovy test-set=1 order=t7,t7Raw,t7Meta,t7RawMeta
import java.text.SimpleDateFormat

t7Raw = newTable(stringCol("Date", "1/1/23", "1/2/23", "1/3/23"))

inputFormat = new SimpleDateFormat("MM/dd/yy")

t7 = t7Raw.update("NewDate = toLocalDate(inputFormat.parse(Date).toInstant(), timeZone())")

t7RawMeta = t7Raw.meta()
t7Meta = t7.meta()
```

Here, the date is stored as an integer, so the example converts it to a `String` with `String.valueOf` before parsing it with [`SimpleDateFormat`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/SimpleDateFormat.html):

```groovy test-set=1 order=t8,t8Raw,t8Meta,t8RawMeta
import java.text.SimpleDateFormat

t8Raw = newTable(intCol("Date", 230101, 230102, 230103))

inputFormat = new SimpleDateFormat("yyMMdd")

t8 = t8Raw.update(
    "NewDate = toLocalDate(inputFormat.parse(String.valueOf(Date)).toInstant(), timeZone())"
)

t8RawMeta = t8Raw.meta()
t8Meta = t8.meta()
```

### Parse times

Much like dates, ISO-formatted time strings convert to `LocalTime` with [`parseLocalTime`](../time/datetime/parseLocalTime.md):

```groovy test-set=1 order=t9,t9Raw,t9Meta,t9RawMeta
t9Raw = newTable(stringCol("Time", "12:30:00", "12:30:01", "12:30:02"))

t9 = t9Raw.update("NewTime = parseLocalTime(Time)")

t9RawMeta = t9Raw.meta()
t9Meta = t9.meta()
```

When the column uses a non-ISO format, create a [`SimpleDateFormat`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/SimpleDateFormat.html) that describes that format. As with [dates](#parse-dates), parsing gives an [`Instant`](../query-language/types/date-time.md#instant), not a time, so the examples convert it back:

- The `SimpleDateFormat` parses a string with no time zone in the [engine time zone](#time-zones).
- [`timeZone`](../time/datetime/timeZone.md) returns that time zone.
- [`toLocalTime`](../time/datetime/toLocalTime.md), given the parsed `Instant` and the result of [`timeZone`](../time/datetime/timeZone.md), returns the original time.

```groovy test-set=1 order=t10,t10Raw,t10Meta,t10RawMeta
import java.text.SimpleDateFormat

t10Raw = newTable(stringCol("Time", "12.30.00", "12.30.01", "12.30.02"))

inputFormat = new SimpleDateFormat("HH.mm.ss")

t10 = t10Raw.update("NewTime = toLocalTime(inputFormat.parse(Time).toInstant(), timeZone())")

t10RawMeta = t10Raw.meta()
t10Meta = t10.meta()
```

Times with no delimiters may be stored as integers. Convert them to strings first:

```groovy test-set=1 order=t11,t11Raw,t11Meta,t11RawMeta
import java.text.SimpleDateFormat

t11Raw = newTable(intCol("Time", 123000, 123001, 123002))

inputFormat = new SimpleDateFormat("HHmmss")

t11 = t11Raw.update(
    "NewTime = toLocalTime(inputFormat.parse(String.valueOf(Time)).toInstant(), timeZone())"
)

t11RawMeta = t11Raw.meta()
t11Meta = t11.meta()
```

## Format date-time display

The [`formatColumns`](../table-operations/format/format-columns.md) table operation changes how a date-time column is displayed. It does not change the underlying data. Reformat timestamps using [valid Java date-time formats](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/SimpleDateFormat.html). The `D` (day of year) pattern is not supported:

```groovy test-set=2 order=t1,t2,t3,t4,t5,t6
t1 = emptyTable(10).update("Timestamp = '2021-01-01T12:30:00Z' + i * 'P1d'")

// double digits for day and month, separated by spaces
t2 = t1.formatColumns("Timestamp = Date(`dd MM y HH:mm:ss`)")

// separate year, month, and day by dashes
t3 = t1.formatColumns("Timestamp = Date(`y-MM-dd HH:mm:ss`)")

// show the month as an abbreviated name
t4 = t1.formatColumns("Timestamp = Date(`LLL d, y HH:mm:ss`)")

// show the day of the week and month as abbreviated names
t5 = t1.formatColumns("Timestamp = Date(`EEE, LLL d, y HH:mm:ss`)")

// only day of month and time
t6 = t1.formatColumns("Timestamp = Date(`dd HH:mm:ss`)")
```

[`formatColumns`](../table-operations/format/format-columns.md) also works on ticking tables. This example formats a `Now` column, computed with [`now`](../time/datetime/now.md), in a [`timeTable`](../table-operations/create/timeTable.md) to show nanosecond precision:

```groovy ticking-table order=null
tNanos = timeTable("PT1S").update("Now = now()").formatColumns("Now = Date(`HH:mm:ss.SSSSSSSSS`)")
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

The [`now`](../time/datetime/now.md), [`today`](../time/datetime/today.md), and [`timeZone`](../time/datetime/timeZone.md) functions return the current time, the current date as a `yyyy-MM-dd` string, and the [engine time zone](#time-zones). Use [`todayLocalDate`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#todayLocalDate()) to get the current date as a `LocalDate`:

```groovy test-set=3
t = emptyTable(1).update(
    "CurrentTime = now()", "CurrentDate = today()", "CurrentTimeZone = timeZone()"
)
```

### Outside of a query

The [`now`](../time/datetime/now.md), [`today`](../time/datetime/today.md), and [`timeZone`](../time/datetime/timeZone.md) functions also work in Groovy code outside query strings:

```groovy test-set=3 order=:log
println "Current time: " + now()
println "Current date: " + today()
println "Current time zone: " + timeZone()
```

## Time zones

The engine time zone is the JVM's default time zone, which is set when the server starts. A `SimpleDateFormat` uses it to parse strings that have no time zone, and [`timeZone`](../time/datetime/timeZone.md) returns it. It is not always the same as the time zone the UI displays, which the [Date-time settings in the UI](#date-time-settings-in-the-ui) section covers.

### Change engine time zone

To change the engine time zone, set the `user.timezone` JVM property at startup, as described in [Set the default time zone](../community-questions/set-timezone.md).

### Time zone aliases

Deephaven provides time zone aliases for some common time zones. The default aliases are `ET`, `CT`, `MT`, `PT`, `HK`, `JP`, and `LON`:

```groovy test-set=4
zones2 = emptyTable(1).update("EasternTimeZone = 'ET'", "PacificTimeZone = 'PT'")
```

[`timeZoneAliasAdd`](../time/datetime/timeZoneAliasAdd.md) adds a custom time zone alias, which queries can then use. [`timeZoneAliasRm`](../time/datetime/timeZoneAliasRm.md) removes it:

```groovy test-set=4
// Add custom time zone alias
timeZoneAliasAdd("WST", "Pacific/Samoa")

zones3 = emptyTable(1).update(
    "EasternTimeZone = 'ET'",
    "PacificTimeZone = 'PT'",
    "SamoaTimeZone1 = 'Pacific/Samoa'",
    "SamoaTimeZone2 = 'WST'",
    // Parsing methods recognize time zone aliases
    "EasternTimeInstant = parseInstant(`2022-04-06T12:50:43 ET`)",
    "PacificTimeInstant = parseInstant(`2022-04-06T12:50:43 PT`)",
    // Custom aliases are also understood
    "SamoaTimeInstant = parseInstant(`2022-04-06T12:50:43 WST`)"
)

// Remove the custom time zone alias. Later queries that use "WST" fail
timeZoneAliasRm("WST")
```

## Manipulate timestamps

The examples in this section use the following table:

```groovy test-set=5
t = emptyTable(2 * 365 * 24 * 60).update(
    "Timestamp = '2020-01-01T00:00:00.00 UTC' + 'PT1m'.multipliedBy(ii)",
    "X = randomGaussian(0, 1)"
)
```

### Convenience methods for date-times

Date-time columns carry a lot of information. Deephaven's [auto-imported functions](../query-language/query-library/auto-imported/index.md) extract it:

```groovy test-set=5
tMoreMethods = t.update(
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
    "SecondsSinceEpoch = epochSeconds(Timestamp)"
)
```

### Bin timestamps

The [`upperBin`](../time/datetime/upperBin.md) and [`lowerBin`](../time/datetime/lowerBin.md) functions group timestamps into bins at regular intervals. Each function accepts the bin interval in either of these forms:

- A number of nanoseconds, such as `5 * SECOND`, which uses a [built-in constant](#built-in-constants).
- A [duration](../query-language/types/durations.md), such as `'PT5S'`.

This example bins `Timestamp` into 10-minute intervals with a duration:

```groovy test-set=5 order=tBinned
tBinned = t.update("BinnedTimestamp = lowerBin(Timestamp, 'PT10m')")
```

[Downsampling](../../how-to-guides/downsampling.md) reduces a table to fewer rows, such as one row per bin. [`firstBy`](../table-operations/group-and-aggregate/firstBy.md), [`lastBy`](../table-operations/group-and-aggregate/lastBy.md), and [`avgBy`](../table-operations/group-and-aggregate/avgBy.md) downsample by grouping on the bin column:

```groovy test-set=5 order=tBinnedFirst,tBinnedLast,tBinnedAvg
// Downsample to first observation every 10 minutes
tBinnedFirst = tBinned.firstBy("BinnedTimestamp")

// Downsample to last observation every 10 minutes
tBinnedLast = tBinned.lastBy("BinnedTimestamp")

// Downsample to average every 10 minutes
tBinnedAvg = tBinned.select("X", "BinnedTimestamp").avgBy("BinnedTimestamp")
```

To perform [multiple aggregations](../../how-to-guides/combined-aggregations.md) at once on binned timestamps, use [`aggBy`](../table-operations/group-and-aggregate/aggBy.md). This example computes the open, high, low, and close of `X` in each 10-minute bin with [`AggFirst`](../table-operations/group-and-aggregate/AggFirst.md), [`AggMax`](../table-operations/group-and-aggregate/AggMax.md), [`AggMin`](../table-operations/group-and-aggregate/AggMin.md), and [`AggLast`](../table-operations/group-and-aggregate/AggLast.md):

```groovy test-set=5
tOhlc = tBinned.aggBy(
    [AggFirst("Open=X"), AggMax("High=X"), AggMin("Low=X"), AggLast("Close=X")],
    "BinnedTimestamp"
)
```

### Time-based filtering

Deephaven's [filter methods](../../how-to-guides/use-filters.md) and [comparison operators](../../how-to-guides/operators.md#comparison-operators) work with timestamps. The last example combines a timestamp comparison with [`Filter.or`](/core/javadoc/io/deephaven/api/filter/Filter.html), which keeps rows that match any of its filters:

```groovy test-set=5 order=t2020,t2021,t2021Summer
// Compare the Timestamp column to an exact timestamp literal
t2020 = t.where("Timestamp < '2021-01-01T00:00:00.00 UTC'")

// Use the built-in function year() in a filter
t2021 = t.where("year(Timestamp, 'UTC') == 2021")

// Built-in functions, methods, literals, and multiple filters
t2021Summer = t.where("Timestamp >= atMidnight('2021-01-01', 'UTC').toInstant()")
    .where(
        Filter.or(
            Filter.from(
                "monthOfYear(Timestamp, 'UTC') == 6",
                "monthOfYear(Timestamp, 'UTC') == 7",
                "monthOfYear(Timestamp, 'UTC') == 8"
            )
        )
    )
```

### Math with date-times

[Arithmetic operators](../../how-to-guides/operators.md#arithmetic-operators) work on date-time values. Adding or subtracting a [duration](../query-language/types/durations.md) shifts an [`Instant`](../query-language/types/date-time.md#instant), and so does a [period](../query-language/types/periods.md) of days or weeks. A period with months or years needs a `ZonedDateTime`. Subtracting one [`Instant`](../query-language/types/date-time.md#instant) from another gives the difference in nanoseconds. Dividing by a [built-in constant](#built-in-constants) such as [`DAY`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#DAY) converts nanoseconds to another unit:

```groovy test-set=5
// Many operations have nanosecond resolution, so conversion to the desired unit is often required
tWithMath = t.update(
    "OneYearAgo = Timestamp - 'P365d'",
    "OneYearNanos = Timestamp - OneYearAgo",
    "OneYear = OneYearNanos / YEAR_365",
    "DaysSinceTimestamp = (now() - Timestamp) / DAY"
)
```

Date-time arithmetic also works directly in [filters](../../how-to-guides/use-filters.md), without new columns:

```groovy test-set=5
// Get data from less than 1000 days before 2024-04-03T15:30:00 UTC
tRecent = t.where("('2024-04-03T15:30:00.00 UTC' - Timestamp) / DAY < 1000")
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

The constants are also available in Groovy code outside of query strings:

```groovy order=:log
println DAY
println HOUR
println MINUTE
println SECOND
println WEEK
println YEAR_AVG
println YEAR_365
```

## Time-based joins

Timestamps from different datasets rarely match exactly, so [exact-match joins](../../how-to-guides/joins-exact-relational.md) on timestamps find few matches. The [`aj`](../table-operations/join/aj.md) and [`raj`](../table-operations/join/raj.md) joins match on the closest timestamp instead. See [Inexact, time-series, and range joins](../../how-to-guides/joins-timeseries-range.md) for more.

The examples in this section join the table `t` from [Manipulate timestamps](#manipulate-timestamps), which has one row per minute and a random `X` column, to a table whose timestamps are offset by random amounts.

### `aj` (as-of join)

The as-of join [`aj`](../table-operations/join/aj.md) joins tables on timestamp columns by choosing values from the right key column that are as close as possible to values in the left key column _without going over_:

```groovy test-set=5 order=asOfJoined,tNoisy
tNoisy = emptyTable(2 * 365 * 24 * 60).update(
    "Timestamp = '2020-01-01T00:00:00.00 UTC' + 'PT1m'.multipliedBy(ii) + 'PT0.1s'.multipliedBy(randomInt(0, 10*60-1))",
    "Y = randomGaussian(0, 1)"
)

asOfJoined = t.aj(tNoisy, "Timestamp", "Y")
```

[`aj`](../table-operations/join/aj.md) can also match one or more key columns exactly before the as-of match. List the timestamp column last among the match columns. This example matches `Group` exactly:

```groovy test-set=5 order=asOfJoinedExact,tGroup,tNoisyGroup
tGroup = t.update("Group = randomInt(0, 5)")
tNoisyGroup = tNoisy.update("Group = randomInt(0, 5)")

asOfJoinedExact = tGroup.aj(tNoisyGroup, "Group, Timestamp", "Y")
```

### `raj` (reverse as-of join)

The reverse as-of join [`raj`](../table-operations/join/raj.md) joins tables on timestamp columns by choosing values from the right key column that are as close as possible to values in the left key column _without going under_. This example uses `tNoisy` from the [`aj` (as-of join)](#aj-as-of-join) section:

```groovy test-set=5 order=reverseAsOfJoined
reverseAsOfJoined = t.raj(tNoisy, "Timestamp", "Y")
```

Like [`aj`](../table-operations/join/aj.md), [`raj`](../table-operations/join/raj.md) can match key columns exactly before the timestamp column. This example reuses `tGroup` and `tNoisyGroup` from the [`aj` (as-of join)](#aj-as-of-join) section:

```groovy test-set=5 order=reverseAsOfJoinedExact
reverseAsOfJoinedExact = tGroup.raj(tNoisyGroup, "Group, Timestamp", "Y")
```

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
- [`AggFirst`](../table-operations/group-and-aggregate/AggFirst.md)
- [`AggLast`](../table-operations/group-and-aggregate/AggLast.md)
- [`AggMax`](../table-operations/group-and-aggregate/AggMax.md)
- [`AggMin`](../table-operations/group-and-aggregate/AggMin.md)
- [`aggBy`](../table-operations/group-and-aggregate/aggBy.md)
- [`aj`](../table-operations/join/aj.md)
- [`atMidnight`](../time/datetime/atMidnight.md)
- [`avgBy`](../table-operations/group-and-aggregate/avgBy.md)
- [`dayOfMonth`](../time/datetime/dayOfMonth.md)
- [`dayOfWeek`](../time/datetime/dayOfWeek.md)
- [`dayOfYear`](../time/datetime/dayOfYear.md)
- [`emptyTable`](../table-operations/create/emptyTable.md)
- [`epochAutoToInstant`](../time/datetime/epochAutoToInstant.md)
- [`epochSeconds`](../time/datetime/epochSeconds.md)
- [`epochSecondsToInstant`](../time/datetime/epochSecondsToInstant.md)
- [`firstBy`](../table-operations/group-and-aggregate/firstBy.md)
- [`formatColumns`](../table-operations/format/format-columns.md)
- [`hourOfDay`](../time/datetime/hourOfDay.md)
- [`instantCol`](../table-operations/create/instantCol.md)
- [`intCol`](../table-operations/create/intCol.md)
- [`lastBy`](../table-operations/group-and-aggregate/lastBy.md)
- [`longCol`](../table-operations/create/longCol.md)
- [`lowerBin`](../time/datetime/lowerBin.md)
- [`meta`](../table-operations/metadata/meta.md)
- [`minuteOfDay`](../time/datetime/minuteOfDay.md)
- [`monthOfYear`](../time/datetime/monthOfYear.md)
- [`newTable`](../table-operations/create/newTable.md)
- [`now`](../time/datetime/now.md)
- [`parseInstant`](../time/datetime/parseInstant.md)
- [`parseLocalDate`](../time/datetime/parseLocalDate.md)
- [`parseLocalTime`](../time/datetime/parseLocalTime.md)
- [`parseTimeZone`](../time/datetime/parseTimeZone.md)
- [`raj`](../table-operations/join/raj.md)
- [`select`](../table-operations/select/select.md)
- [`stringCol`](../table-operations/create/stringCol.md)
- [`timeTable`](../table-operations/create/timeTable.md)
- [`timeZone`](../time/datetime/timeZone.md)
- [`timeZoneAliasAdd`](../time/datetime/timeZoneAliasAdd.md)
- [`timeZoneAliasRm`](../time/datetime/timeZoneAliasRm.md)
- [`today`](../time/datetime/today.md)
- [`toInstant`](../time/datetime/toInstant.md)
- [`toLocalDate`](../time/datetime/toLocalDate.md)
- [`toLocalTime`](../time/datetime/toLocalTime.md)
- [`update`](../table-operations/select/update.md)
- [`upperBin`](../time/datetime/upperBin.md)
- [`where`](../table-operations/filter/where.md)
- [`year`](../time/datetime/year.md)
- [`DateTimeUtils` Javadoc](/core/javadoc/io/deephaven/time/DateTimeUtils.html)
- [`Filter` Javadoc](/core/javadoc/io/deephaven/api/filter/Filter.html)
