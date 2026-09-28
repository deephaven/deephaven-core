---
title: How do I filter a table by time of day across all dates?
sidebar_label: How do I filter by time of day across all dates?
---

_I have a table with a timestamp column that spans multiple dates. I want to filter rows to only include times between 2:00 PM and 4:00 PM ET, regardless of the date._

Use the [`hourOfDay`](https://docs.deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#hourOfDay(java.time.Instant,java.time.ZoneId,boolean)) function to extract the hour component from your timestamp, then filter based on the hour value:

```groovy test-set=time-filter order=t,result
import static io.deephaven.time.DateTimeUtils.*

et = timeZone("ET")
startTime = parseInstant("2024-01-15T13:00:00 ET")

// Sample table with various timestamps across different dates
t = emptyTable(10).update(
    "Timestamp = startTime + i * 30 * MINUTE"
)

// Filter for times between 2:00 PM and 4:00 PM ET
result = t.where("hourOfDay(Timestamp, et, false) >= 14 && hourOfDay(Timestamp, et, false) < 16")
```

This approach works by:

1. Using [`hourOfDay(Timestamp, et, false)`](https://docs.deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#hourOfDay(java.time.Instant,java.time.ZoneId,boolean)) to extract the hour (0-23) from the timestamp in ET timezone.
2. Filtering for hours between 14 (2:00 PM) and 15 (3:00 PM, up to but not including 4:00 PM).
3. The condition checks that the hour is greater than or equal to 14 and less than 16, ensuring times from 2:00:00 PM through 3:59:59 PM are included.

## Filtering with minutes

If you need more precise time filtering (e.g., 2:30 PM to 4:15 PM), combine [`hourOfDay`](https://docs.deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#hourOfDay(java.time.Instant,java.time.ZoneId,boolean)) and [`minuteOfHour`](https://docs.deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#minuteOfHour(java.time.Instant,java.time.ZoneId)):

```groovy test-set=time-filter order=t2,result2
startTime2 = parseInstant("2024-01-15T14:15:00 ET")

t2 = emptyTable(10).update(
    "Timestamp = startTime2 + i * 15 * MINUTE"
)

// Filter for times between 2:30 PM and 4:15 PM ET
result2 = t2.where(
    "(hourOfDay(Timestamp, et, false) == 14 && minuteOfHour(Timestamp, et) >= 30) || " +
    "(hourOfDay(Timestamp, et, false) == 15) || " +
    "(hourOfDay(Timestamp, et, false) == 16 && minuteOfHour(Timestamp, et) <= 15)"
)
```

This filter includes:

- Hour 14 (2:00 PM) with minutes greater than or equal to 30.
- All of hour 15 (3:00 PM).
- Hour 16 (4:00 PM) with minutes less than or equal to 15.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
