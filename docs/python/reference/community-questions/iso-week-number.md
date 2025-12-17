---
title: How do I get the ISO week number from a datetime?
sidebar_label: How do I get the ISO week number from a datetime?
---

ISO week numbers (sometimes called ISO week dates) are defined by the ISO 8601 standard. The ISO week number represents the week of the year, with weeks starting on Monday and the first week of the year being the week that contains the first Thursday of the year.

```python test-set=1 order=t,t2
from deephaven import empty_table
from deephaven.time import to_j_time_zone

et = to_j_time_zone("ET")

t = empty_table(1).update("Timestamp = now()")

t2 = t.update(
    [
        "WeekNum = (int)((dayOfYear(Timestamp, et) - dayOfWeekValue(Timestamp, et) + 10) / 7)",
    ]
)
```

The code above calculates a week number using the following formula:

```syntax
WeekNum = (dayOfYear - dayOfWeekValue + 10) / 7
```

Where:

- `dayOfYear(Timestamp, et)` returns the day of the year (1-366).
- `dayOfWeekValue(Timestamp, et)` returns the day of the week as a number (1=Monday, 7=Sunday).
- The `+ 10` adjustment helps align the calculation.
- The result is divided by 7 and cast to an integer.

This formula provides an approximation of week numbering but does not strictly follow ISO 8601 standards. It calculates weeks based on the day of the year adjusted for the day of the week, which can be useful for general week-based grouping of data.

## Understanding ISO week numbering

The ISO week numbering has some important characteristics:

1. Weeks always start on Monday.
2. Week 1 is the week containing the first Thursday of the year.
3. Some years can have 53 weeks according to ISO standards.

This means that:

- The last few days of December might be part of week 1 of the next year.
- The first few days of January might be part of week 52 or 53 of the previous year.

For example, January 1, 2024 is in ISO week 1 of 2024, but December 31, 2023 is in ISO week 52 of 2023.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
