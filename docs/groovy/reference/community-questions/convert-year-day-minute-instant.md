---
title: How can I convert year, month, day, and minute to an Instant?
---

_I have a table with columns containing an integer year, month of year, day of month, and minute of day. How can I convert these columns to a single Instant column?_

> [!NOTE]
> The example on this page uses the `ET` time zone. To use a different time zone, replace `'ET'` in the formula.

Build a [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html) from the integer columns, convert the minute of day to a [`LocalTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalTime.html), then combine them into an [`Instant`](../time/datetime/toInstant.md) with a time zone. Adding the minute as a wall-clock time (rather than elapsed nanoseconds) ensures correct results on DST transition days.

```groovy test-set=1 order=result
source = emptyTable(1).update("Year = 2024", "Month = 3", "Day = 4", "Minute = 720")

result = source.update(
    "Timestamp = toInstant(LocalDate.of(Year, Month, Day), LocalTime.ofSecondOfDay(Minute * 60L), 'ET')"
)
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
