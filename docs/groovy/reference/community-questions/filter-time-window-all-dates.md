---
title: How do I filter a table by time of day across all dates?
---

_I have a table with a timestamp column that spans multiple dates. I want to filter rows to only include times between 2:00 PM and 4:00 PM ET, regardless of the date._

Use the [`hourOfDay`](../time/datetime/hourOfDay.md) function to extract the hour component from your timestamp, then filter based on the hour value:

```groovy test-set=time-filter order=t,result
import static io.deephaven.time.DateTimeUtils.*

et = timeZone("ET")
startTime = parseInstant("2024-01-14T08:00:00 ET")

// Sample table with timestamps across several days
t = emptyTable(20).update(
    "Timestamp = startTime + i * 7 * HOUR"
)

// Filter for times between 2:00 PM and 4:00 PM ET
result = t.where("hourOfDay(Timestamp, et, true) >= 14 && hourOfDay(Timestamp, et, true) < 16")
```

This approach works by:

1. Using [`hourOfDay(Timestamp, et, true)`](../time/datetime/hourOfDay.md) to extract the hour (0-23) from the timestamp in the ET time zone.
2. The condition `hour >= 14 && hour < 16` keeps rows from 2:00:00 PM through 3:59:59 PM.

Passing `true` for the `asLocalTime` parameter returns the local clock hour, so the filter stays correct on daylight saving time change days.

## Filtering with minutes

If you need more precise time filtering (e.g., 2:30 PM to 4:15 PM), combine [`hourOfDay`](../time/datetime/hourOfDay.md) and [`minuteOfHour`](../time/datetime/minuteOfHour.md):

```groovy test-set=time-filter order=t2,result2
startTime2 = parseInstant("2024-01-14T10:00:00 ET")

t2 = emptyTable(20).update(
    "Timestamp = startTime2 + i * 7 * HOUR"
)

// Filter for times between 2:30 PM and 4:15 PM ET
result2 = t2.where(
    "(hourOfDay(Timestamp, et, true) == 14 && minuteOfHour(Timestamp, et) >= 30) || " +
    "(hourOfDay(Timestamp, et, true) == 15) || " +
    "(hourOfDay(Timestamp, et, true) == 16 && minuteOfHour(Timestamp, et) <= 15)"
)
```

This filter includes:

- Hour 14 (2:00 PM) with minutes greater than or equal to 30.
- All of hour 15 (3:00 PM).
- Hour 16 (4:00 PM) with minutes less than or equal to 15.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
