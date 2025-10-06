---
title: How can I bin times to a specific time?
sidebar_label: How can I bin times to a specific time?
---

_I have a table with time series data. I know I can bin my timestamps into fixed-duration bins. I need these bins to start at a specific time of day. How can I do this?_

The Deephaven query library's built-in [`DateTimeUtils`](/core/javadoc/io/deephaven/time/DateTimeUtils.html) class has the methods [`lowerBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long,long)) and [`upperBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long,long)) that can be used to bin your time series data. They allow you to pass an offset so that the bins start later than they normally would. You can use this offset to get the bins to start at a specific time.

`lowerBin` and `upperBin` calculate bin start/end times using [Unix time](https://en.wikipedia.org/wiki/Unix_time). So, if you bin times in 5-minute intervals, the bin start/end times are an integer multiple of 5 minutes since the Unix epoch (`1970-01-01T00:00:00 UTC`).

Getting time bins to start at a specific time can be done in several ways. We will provide solutions that cover the following two cases:

- You want the bins to snap to a specific time on a single day.
- You want the bins to snap to a specific time _every_ day.

For the examples below, we will provide code that makes time bins start at 9:30AM ET (NYSE trading start time). We will also only provide solutions that do this for bins that are an integer number of minutes long. The solutions can be extrapolated to cover cases where the bins don't meet this criteria.

> [!IMPORTANT]
> Bin start times are calculated using Unix epoch time, which is in the UTC time zone. ET (standard time) is 5 hours behind, so NYSE trading start is actually 2:30PM UTC.

## Bin without an offset

Bin sizes of 1, 2, 3, 5, 10, 15, and 30 minutes will _always_ snap to 9:30AM ET every day, since they are factors of 24 hours. Bin sizes, such as 7 minutes, that are not factors of 24 hours will not always snap to the specific time:

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

Notice how `LowerBin7Min` and `UpperBin7Min` don't snap to 9:30AM ET; rather they snap to 9:24AM ET. To get these to snap to 9:30, you would need to provide a 6-minute offset as the third input parameter to `lowerBin` and `upperBin`. This offset will change every day, so a 6-minute offset will only work for some days.

## Single day

As previously mentioned, the offset needed for snapping bins to a specific time depends on the day. So, we need to be able to calculate this offset for any given day. To do this, we compute the total time from the Epoch to the specific "snapping" time, and divide this by the length of the time bins. Whatever remainder is left over from that division is the offset needed for that day. This can be done via Python's built-in [datetime](https://docs.python.org/3/library/datetime.html) library. Let's do this for 3 different bin sizes: 7 minutes, 14 minutes, and 19 minutes.

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

You can use these values in a `lowerBin` or `upperBin`:

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

Bin sizes like 7, 14, and 19 minutes don't multiply evenly into 24 hours. The code above will only get bins to snap to that time for a single day. You can make this happen for every day instead. Just keep in mind that bins that contain midnight (a change in days) will likely not be the correct size.

In order to get this to happen every day, you need to use the equivalent methods from the [`Numeric`](/core/javadoc/io/deephaven/function/Numeric.html) class instead. If you pass in the correct data types, the engine knows to use these methods rather than the ones in [`DateTimeUtils`](/core/javadoc/io/deephaven/time/DateTimeUtils.html).

- [`lowerBin`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#lowerBin(long,long,long))
- [`upperBin`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#upperBin(long,long,long))

To do this for time series data, you'll need to extract the minute of day from your Instant column. We also extract the year, month, and day of month so we can [convert the result](./convert-year-day-minute-instant.md) to an Instant back after calculations.

```python test-set=1 order=result
from deephaven.time import to_j_time_zone

et_tz = to_j_time_zone("ET")

result = source.update(
    [
        "Year = year(Timestamp, et_tz)",
        "Month = monthOfYear(Timestamp, et_tz)",
        "Day = dayOfMonth(Timestamp, et_tz)",
        "Minute = minuteOfDay(Timestamp, et_tz, false)",
    ]
)
```

Since this data is now stored for the correct time zone, we don't need to account for a difference in time zones. NYSE trading start time is 570 minutes into the day.

```python test-set=1 order=result_binned
def calc_bin_offset(binsize, start_minute):
    return int(start_minute % binsize)


offset_7min = calc_bin_offset(7, 570)
offset_14min = calc_bin_offset(14, 570)
offset_19min = calc_bin_offset(19, 570)

print(f"7 minute bin: {offset_7min} minute offset.")
print(f"14 minute bin: {offset_14min} minute offset.")
print(f"19 minute bin: {offset_19min} minute offset.")

result_binned = result.update(
    [
        f"LowerBin7Min = lowerBin(Minute, 7, {offset_7min})",
        f"UpperBin14Min = upperBin(Minute, 14, {offset_14min})",
        f"LowerBin19Min = lowerBin(Minute, 19, {offset_19min})",
    ]
)
```

Just to prove that it works for more than just a single day:

```python test-set=1 order=new_result_binned,new_source
new_start_time = to_j_instant("2024-01-02T09:20:00 ET")

new_source = empty_table(20).update(
    [
        "Timestamp = new_start_time + i * MINUTE",
        "Year = year(Timestamp, et_tz)",
        "Month = monthOfYear(Timestamp, et_tz)",
        "Day = dayOfMonth(Timestamp, et_tz)",
        "Minute = minuteOfDay(Timestamp, et_tz, false)",
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

You can aggregate on these bins just like you would an Instant column. Or, you could [convert the data back to an Instant](./convert-year-day-minute-instant.md).

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
