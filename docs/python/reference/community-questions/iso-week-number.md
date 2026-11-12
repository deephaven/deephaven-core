---
title: How do I get the ISO week number from a datetime?
---

The ISO week number is the week component of an [ISO 8601](https://en.wikipedia.org/wiki/ISO_week_date) week date (for example, the 29 in 2025-W29-4).

ISO week numbering has some important characteristics:

1. Weeks always start on Monday.
2. Week 1 is the week containing the first Thursday of the year.
3. Some years have 53 weeks according to ISO standards.

This means that:

- The last few days of December can be part of week 1 of the next year. For example, December 30, 2024 is in ISO week 1 of 2025.
- The first few days of January can be part of week 52 or 53 of the previous year. For example, January 1, 2021 is in ISO week 53 of 2020.

The following formula computes the ISO week number from a timestamp:

```python test-set=1 order=source,result
from deephaven import new_table
from deephaven.column import datetime_col
from deephaven.time import to_j_time_zone, to_j_instant

et = to_j_time_zone("ET")

source = new_table(
    [
        datetime_col(
            "Timestamp",
            [
                to_j_instant("2025-07-17T12:00:00 ET"),
                to_j_instant("2024-12-30T12:00:00 ET"),
                to_j_instant("2021-01-01T12:00:00 ET"),
            ],
        ),
    ]
)

result = source.update(
    [
        "WeekNum = (int)((dayOfYear(Timestamp, et) - dayOfWeekValue(Timestamp, et) + 10) / 7)",
    ]
)
```

Where:

- [`dayOfYear(Timestamp, et)`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#dayOfYear(java.time.Instant,java.time.ZoneId)) returns the day of the year (1-366).
- [`dayOfWeekValue(Timestamp, et)`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#dayOfWeekValue(java.time.Instant,java.time.ZoneId)) returns the day of the week as a number (1=Monday, 7=Sunday).
- `dayOfYear - dayOfWeekValue + 4` is the day-of-year of that week's Thursday. Adding 6 more and dividing by 7 converts that Thursday into a 1-based week number.

This formula matches ISO 8601 for most dates. At year boundaries it can return 0 (the date belongs to the last week of the previous year) or 53 when the date belongs to week 1 of the next year. The example above shows both cases: January 1, 2021 returns 0 (it is in ISO week 53 of 2020), and December 30, 2024 returns 53 (it is in ISO week 1 of 2025).

For an exact ISO week number on every date, use `java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR`:

```python test-set=1 order=result_exact
result_exact = source.update(
    [
        "WeekNum = toZonedDateTime(Timestamp, et).get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR)",
    ]
)
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
