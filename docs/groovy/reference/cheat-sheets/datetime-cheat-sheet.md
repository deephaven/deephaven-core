---
title: Time operations cheat sheet
sidebar_label: Time operations
---

## Formats

### hh:mm:ss

Deephaven [time tables](../table-operations/create/timeTable.md) use timestamps in the `"PTH##M##S##N##` format to specify ticking intervals.

```groovy
result = timeTable(now(), "PT1S")
```

### yyyy-mm-ddT:hh:mm:ss.[millis|micros|nanos] TZ

Deephaven [time tables](../table-operations/create/timeTable.md) may also use the `yyyy-mm-ddT:hh:mm:ss.[millis|micros|nanos] TZ` format to specify a start date.

`TZ` represents the time zone. A few examples are:

- `UTC`
- `ET` (New York)
- `JP` (Tokyo)

```groovy
result = timeTable(now(), "PT1S")
```

## Date-time constants

Deephaven comes with some predefined constants:

- `DAY`
- `HOUR`
- `MINUTE`
- `SECOND`
- `WEEK`
- `YEAR`

These constants all show the time in nanoseconds.

```groovy
println DAY
println HOUR
println MINUTE
println SECOND
println WEEK
println YEAR_AVG
println YEAR_365
```

See the [Javadoc](/core/javadoc/io/deephaven/time/DateTimeUtils.html) for further details.

## Convert string to Deephaven date-time object

Deephaven supports converting a string to a Deephaven date-time object via the [`parseInstant`] method.

Strings should be in the `yyyy-mm-ddT:hh:mm:ss.[millis|micros|nanos] TZ` format.

```groovy
dateTimeObject = parseInstant("2021-07-04T08:00:00 ET")
println dateTimeObject.getClass()
```

## Convert nanoseconds to Deephaven date-time object

Deephaven supports converting a long representing nanoseconds since the Epoch to a Deephaven date-time object via the [`autoEpochToInstant`] method.

```groovy
dateTimeObject = epochAutoToInstant(1631045972)
```

## Date-time columns

Deephaven tables have built-in support for Deephaven date-time objects via the [`instantCol`](../table-operations/create/instantCol.md) method.

```groovy
firstTime = parseInstant("2021-07-04T08:00:00 ET")
secondTime = parseInstant("2021-09-06T12:30:00 ET")
thirdTime = parseInstant("2021-12-25T21:15:00 ET")

result = newTable(
    instantCol("DateTimes", firstTime, secondTime, thirdTime)
)
```

## Timestamp comparison

Deephaven's filtering has support for timestamp comparisons.

> [!NOTE]
> Query strings require single quotes `'` around timestamps.

```groovy order=result
firstTime = parseInstant("2021-07-04T08:00:00 UTC")
secondTime = parseInstant("2021-09-06T12:30:00 UTC")
thirdTime = parseInstant("2021-12-25T21:15:00 UTC")

result = newTable(
    instantCol("DateTimes", firstTime, secondTime, thirdTime)
)

filtered = result.where("DateTimes >= '2021-09-06T12:30:00 UTC'")
```

See our [How to use filters](../../how-to-guides/filters.md) guide for more information.

## Time zones

```groovy
dateTimeObject = parseInstant("2021-07-04T08:00:00 ET")

dayOfMonth = dayOfMonth(dateTimeObject, timeZone("ET"))
println dayOfMonth
```

## Downsampling temporal data via time binning

Downsampling time series data may be accomplished by calculating binning-intervals for time values and using appropriate aggregation methods, grouped by the binned interval.

```groovy order=dataTable,binnedTable,groupedUpper,groupedLower
dataTable = timeTable("2023-02-06T12:30:09 ET", "P4D")

binnedTable = dataTable.updateView("UpperBin=upperBin(Timestamp, 5 * SECOND)", "LowerBin=lowerBin(Timestamp, 5 * SECOND)")
groupedUpper = binnedTable
    .aggBy([AggFirst("FirstDateTime=Timestamp"),AggLast("LastDateTime=Timestamp")], "UpperBin")

groupedLower = binnedTable
    .aggBy([AggFirst("FirstDateTime=Timestamp"),AggLast("LastDateTime=Timestamp")], "LowerBin")
```
