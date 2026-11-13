---
title: How can I bin times to a specific time?
---

_I have a table with time series data. I know I can bin my timestamps into fixed-duration bins. I need these bins to start at a specific time of day. How can I do this?_

The Deephaven query library's built-in [`DateTimeUtils`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html) class has the methods [`lowerBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long,long)) and [`upperBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long,long)) that can be used to bin your time series data. They allow you to pass an offset so that the bins start later than they normally would. You can use this offset to get the bins to start at a specific time.

[`lowerBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long,long)) and [`upperBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long,long)) calculate bin start/end times using [Unix time](https://en.wikipedia.org/wiki/Unix_time). So, if you bin times in 5-minute intervals, the bin start/end times are an integer multiple of 5 minutes since the Unix epoch (`1970-01-01T00:00:00 UTC`).

Getting time bins to start at a specific time can be done in several ways. This page covers two cases:

- You want the bins to snap to a specific time on a single day.
- You want the bins to snap to a specific time _every_ day.

The examples below make time bins start at 9:30AM ET (NYSE trading start time). They only cover bins that are an integer number of minutes long. The solutions can be extrapolated to cover cases where the bins don't meet this criterion.

## Bin without an offset

Bin sizes that evenly divide 30 minutes (1, 2, 3, 5, 6, 10, 15, and 30 minutes) always snap to 9:30AM ET, because 9:30AM ET is a whole number of 30-minute periods after midnight UTC in both standard and daylight time. Other bin sizes, such as 7 minutes, do not always snap to 9:30AM ET:

```python test-set=1 order=result,source
from deephaven.time import to_j_instant
from deephaven import empty_table

start_time = to_j_instant("2024-01-02T09:20:00 ET")

source = empty_table(20).update(["Timestamp = start_time + i * MINUTE"])

result = source.update(
    [
        "LowerBin5Min = lowerBin(Timestamp, 5 * MINUTE)",
        "UpperBin5Min = upperBin(Timestamp, 5 * MINUTE)",
        "LowerBin7Min = lowerBin(Timestamp, 7 * MINUTE)",
        "UpperBin7Min = upperBin(Timestamp, 7 * MINUTE)",
    ]
)
```

Notice how `LowerBin7Min` and `UpperBin7Min` don't snap to 9:30AM ET; rather they snap to 9:24AM ET. To get these to snap to 9:30, you need to provide a 6-minute offset as the third input parameter to [`lowerBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long,long)) and [`upperBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long,long)). This offset changes every day, so a 6-minute offset only works for some days.

## Single day

> [!IMPORTANT]
> Bin start times are calculated using Unix epoch time, which is in the UTC time zone. ET (standard time, EST) is 5 hours behind UTC, so NYSE trading start is 2:30PM UTC. During daylight saving time (EDT), ET is 4 hours behind UTC, so 9:30AM ET is 1:30PM UTC. The `calc_bin_offset` function below takes UTC hours, so pass `14, 30` for dates in EST and `13, 30` for dates in EDT.

The offset needed for snapping bins to a specific time depends on the day. So, we need to calculate this offset for any given day. To do this, compute the total time from the Unix epoch to the specific "snapping" time, and divide this by the length of the time bins. The remainder from that division is the offset needed for that day. This can be done via Python's built-in [datetime](https://docs.python.org/3/library/datetime.html) library. Here is the calculation for 3 different bin sizes: 7 minutes, 14 minutes, and 19 minutes.

```python test-set=1 order=:log
import datetime


def calc_bin_offset(year, month, day, hour, minute, binsize):
    bin_start_time = datetime.datetime(year, month, day, hour, minute, 0)
    epoch_time = datetime.datetime(1970, 1, 1, 0, 0, 0)
    minutes_since_epoch = int((bin_start_time - epoch_time).total_seconds() / 60)
    return int(minutes_since_epoch % binsize)


offset_7min = calc_bin_offset(2024, 1, 2, 14, 30, 7)
offset_14min = calc_bin_offset(2024, 1, 2, 14, 30, 14)
offset_19min = calc_bin_offset(2024, 1, 2, 14, 30, 19)

print(f"7 minute bin: {offset_7min} minute offset.")
print(f"14 minute bin: {offset_14min} minute offset.")
print(f"19 minute bin: {offset_19min} minute offset.")
```

You can use these values in [`lowerBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long,long)) or [`upperBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long,long)):

```python test-set=1 order=result
result = source.update(
    [
        f"LowerBin7Min = lowerBin(Timestamp, 7 * MINUTE, {offset_7min} * MINUTE)",
        f"UpperBin7Min = upperBin(Timestamp, 7 * MINUTE, {offset_7min} * MINUTE)",
        f"LowerBin14Min = lowerBin(Timestamp, 14 * MINUTE, {offset_14min} * MINUTE)",
        f"UpperBin14Min = upperBin(Timestamp, 14 * MINUTE, {offset_14min} * MINUTE)",
        f"LowerBin19Min = lowerBin(Timestamp, 19 * MINUTE, {offset_19min} * MINUTE)",
        f"UpperBin19Min = upperBin(Timestamp, 19 * MINUTE, {offset_19min} * MINUTE)",
    ]
)
```

Now, each column has bins that start at 9:30AM ET.

## Every day

Bin sizes like 7, 14, and 19 minutes don't divide evenly into 24 hours. The offsets computed in [Single day](#single-day) make bins start at 9:30AM ET on one date only. To make bins start at 9:30AM ET every day, bin on the minute of the day instead of on the timestamp. Bins that contain midnight are likely to be shorter than the bin size.

To do this, use the equivalent methods from the [`Numeric`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html) class instead. If you pass in the correct data types, the engine uses these methods rather than the ones in [`DateTimeUtils`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html).

- [`lowerBin`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#lowerBin(long,long,long))
- [`upperBin`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#upperBin(long,long,long))

Extract the minute of day from your [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) column. Also extract the year, month, and day of month so you can [convert the result back to an `Instant`](./convert-year-day-minute-instant.md) after calculations.

```python test-set=1 order=result
from deephaven.time import to_j_time_zone

et_tz = to_j_time_zone("ET")

result = source.update(
    [
        "Year = year(Timestamp, et_tz)",
        "Month = monthOfYear(Timestamp, et_tz)",
        "Day = dayOfMonth(Timestamp, et_tz)",
        "Minute = minuteOfDay(Timestamp, et_tz, true)",
    ]
)
```

Passing `true` for the `asLocalTime` parameter measures minutes by the local clock, so 9:30AM is always minute 570, even on daylight saving time change days. Note that these are wall-clock bins: during a fall-back transition, two different instants (e.g., 1:30 AM EDT and 1:30 AM EST) produce the same minute-of-day label, so bins in the overlap hour contain data from both offsets.

Since this data is now stored for the correct time zone, we don't need to account for a difference in time zones. NYSE trading start time is 570 minutes into the day.

```python test-set=1 order=result_binned
def calc_daily_bin_offset(binsize, start_minute):
    return int(start_minute % binsize)


offset_7min = calc_daily_bin_offset(7, 570)
offset_14min = calc_daily_bin_offset(14, 570)
offset_19min = calc_daily_bin_offset(19, 570)

print(f"7 minute bin: {offset_7min} minute offset.")
print(f"14 minute bin: {offset_14min} minute offset.")
print(f"19 minute bin: {offset_19min} minute offset.")

result_binned = result.update(
    [
        f"LowerBin7Min = lowerBin(Minute, 7, {offset_7min})",
        f"LowerBin14Min = lowerBin(Minute, 14, {offset_14min})",
        f"LowerBin19Min = lowerBin(Minute, 19, {offset_19min})",
    ]
)
```

To verify that this works across multiple days:

```python test-set=1 order=new_result_binned,new_source
new_start_time = to_j_instant("2024-01-05T09:20:00 ET")

new_source = empty_table(60).update(
    [
        "Timestamp = new_start_time + i * HOUR",
        "Year = year(Timestamp, et_tz)",
        "Month = monthOfYear(Timestamp, et_tz)",
        "Day = dayOfMonth(Timestamp, et_tz)",
        "Minute = minuteOfDay(Timestamp, et_tz, true)",
    ]
)

new_result_binned = new_source.update(
    [
        f"LowerBin7Min = lowerBin(Minute, 7, {offset_7min})",
        f"LowerBin14Min = lowerBin(Minute, 14, {offset_14min})",
        f"LowerBin19Min = lowerBin(Minute, 19, {offset_19min})",
    ]
)
```

Since these integer bin labels repeat every day, include the date columns (`Year`, `Month`, `Day`) as grouping keys when aggregating to keep each day's bins separate. You can also [convert the data back to an `Instant`](./convert-year-day-minute-instant.md).

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
