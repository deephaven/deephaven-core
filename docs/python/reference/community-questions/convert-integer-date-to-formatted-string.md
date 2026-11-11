---
title: How can I convert an integer date to a formatted date string?
---

_I have a column containing dates as integers (like 20250401). How can I convert these to formatted date strings (YYYY-MM-DD) in Deephaven?_

Extract the year, month, and day components from the integer, then use [`String.format`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html#format(java.lang.String,java.lang.Object...)) to build an ISO date string. If you also need a date value for date arithmetic or time functions, parse the string into a [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html) with [`parseLocalDate`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseLocalDate(java.lang.String)):

```python order=source,result
from deephaven import new_table
from deephaven.column import int_col

source = new_table([int_col("DateInt", [20230101, 20240215, 20250401, 20260630])])

result = source.update(
    [
        "Year = (int)(DateInt / 10000)",
        "Month = (int)((DateInt % 10000) / 100)",
        "Day = (int)(DateInt % 100)",
        "DateString = String.format(`%04d-%02d-%02d`, Year, Month, Day)",
        "Date = parseLocalDate(DateString)",
    ]
)
```

This approach works by:

1. Extracting the year by dividing the integer by 10000 and casting to `int` to drop the fractional part (in query strings, `/` on two integers returns a `double`; for example, 20250401 / 10000 = 2025.0401, which becomes 2025).
2. Extracting the month by taking the remainder after dividing by 10000, then dividing by 100 and casting to `int` (e.g., 20250401 % 10000 = 401, then 401 / 100 = 4.01, which becomes 4).
3. Extracting the day by taking the remainder after dividing by 100 (e.g., 20250401 % 100 = 1).
4. Using [`String.format`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html#format(java.lang.String,java.lang.Object...)) to create a properly formatted ISO date string (`YYYY-MM-DD`).
5. Using [`parseLocalDate`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseLocalDate(java.lang.String)) to convert the string to a [`LocalDate`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/LocalDate.html) object.

`DateString` is the formatted string. `Date` is a `LocalDate` value that works with Deephaven's other date and time functions. A month or day that is fully out of range (such as 13 or 32) raises an error, but a day past the end of its month (such as Feb 30) is adjusted to the last valid day rather than rejected.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
