---
title: How do I extract a list of distinct dates from a table?
sidebar_label: How do I extract a list of distinct dates from a table?
---

_I have a table with a date column that contains many repeated dates. How can I get a list of the unique dates?_

Use [`selectDistinct`](../table-operations/filter/select-distinct.md) to get the unique dates:

```groovy test-set=dates order=source,distinctDates
import static io.deephaven.time.DateTimeUtils.parseInstant

// Sample table with many rows and repeated dates across multiple timestamps
startDate = parseInstant("2024-01-15T09:00:00 ET")

source = emptyTable(50).update(
    "Timestamp = startDate + (long)(i * HOUR)",
    "Date = toLocalDate(Timestamp, 'ET')",
    "Value = randomDouble(100.0, 500.0)"
)

// Get distinct dates - reduces 50 rows to just the unique dates
distinctDates = source.selectDistinct("Date")
```

The `distinctDates` table now contains only the unique dates from the original table. You can view this table in the Deephaven IDE or use it in further table operations.

This approach works for any column type, not just dates. Use `selectDistinct` to get unique values from any column in your table.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
