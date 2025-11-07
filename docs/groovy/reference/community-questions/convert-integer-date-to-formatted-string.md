---
title: How can I convert an integer date to a formatted date string?
sidebar_label: How can I convert an integer date to a formatted string?
---

_I have a column containing dates as integers (like 20250401). How can I convert these to formatted date strings (YYYY-MM-DD) in Deephaven?_

The best practice is to first format the integer as a proper date string, then use Deephaven's built-in [`parseLocalDate`](https://docs.deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseLocalDate(java.lang.String)) function:

```groovy order=source,result
// Sample table with integer dates
def source = newTable(
    intCol("DateInt", 20230101, 20240215, 20250401, 20260630)
)

// Best practice: Format as string, then parse as LocalDate
def result = source.update(
    "Year = (int)(DateInt / 10000)",
    "Month = (int)((DateInt % 10000) / 100)",
    "Day = (int)(DateInt % 100)",
    "DateString = String.format(\"%04d-%02d-%02d\", Year, Month, Day)",
    "DateFormatted = parseLocalDate(DateString)"
)
```

This approach works by:

1. Extracting the year by dividing the integer by 10000 (e.g., 20250401 / 10000 = 2025).
2. Extracting the month by taking the remainder after dividing by 10000, then dividing by 100 (e.g., 20250401 % 10000 = 401, then 401 / 100 = 4).
3. Extracting the day by taking the remainder after dividing by 100 (e.g., 20250401 % 100 = 1).
4. Using `String.format` to create a properly formatted ISO date string (YYYY-MM-DD).
5. Using [`parseLocalDate`](https://docs.deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseLocalDate(java.lang.String)) to convert the string to a proper [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html) object.

This is the recommended approach because:

- It creates proper [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html) objects that can be used for further date operations.
- It handles date validation automatically through [`parseLocalDate`](https://docs.deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseLocalDate(java.lang.String)).
- It follows Deephaven best practices by using built-in parsing functions.
- The resulting [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html) objects work seamlessly with other time-based functions.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
