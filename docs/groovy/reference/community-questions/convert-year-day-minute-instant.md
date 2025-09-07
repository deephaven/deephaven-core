---
title: How can I convert year, day, and minute to an Instant?
sidebar_label: How can I convert year, day, and minute to an Instant?
---

_I have a table with columns containing an integer year, month of year, day of month, and minute of day. How can I convert these columns to a single Instant column?_

> [!NOTE]
> The example provided in this page uses the `ET` time zone. Adjust your code accordingly if you wish to use a different one.

Deephaven's query engine provides the necessary methods to convert integer year, month, day, and minute to an Instant in a given time zone.

```groovy test-set=1 order=result
source = emptyTable(1).update("Year = 2024", "Month = 3", "Day = 4", "Minute = 720")

result = source.update(
    "Timestamp = LocalDate.of(Year, Month, Day).atTime(0, 0, 0).atZone('ET') + Minute * MINUTE"
)
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
