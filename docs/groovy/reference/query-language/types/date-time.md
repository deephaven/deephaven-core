---
title: DateTime
---

A `date-time` value indicates a specific instance in time. In Deephaven, a `date-time` is typically represented by one of two data types:

- [`java.time.Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html)
- [`java.time.ZonedDateTime`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZonedDateTime.html)

These data types are used by Deephaven tables to represent moments in time. They are also used in the Python API. Both will be covered in this reference guide.

## Instant

A `java.time.Instant` represents a single instantaneous point in time, given in the `UTC` time zone. Printing an instance of an instant always ends in the letter `Z`, which is shorthand for `UTC`.

## ZonedDateTime

A `java.time.ZonedDateTime` represents a single instantaneous point in time, given in the specified time zone. Printing an instance of a zoned date-time always ends in the specified time zone. For instance, for the `ET` (US Eastern Time) time zone, a `ZonedDateTime` ends in `[America/New_York]`.

## Syntax

`'YYYY-MM-DDThh:mm:ss.ffffff TZ'`

- `YYYY` - the year
- `MM` - the month
- `DD` - the day
- `T` - the separator between the date and time
- `hh` - the hour of the day
- `mm` - the minute of the hour
- `ss` - the second of the minute
- `ffffff` - the fraction of a second
- `TZ` - the time zone

## Example

The following example creates a table containing date-times and then filters the table based upon a date-time defined in a query string.

```groovy order=source,result
firstTime = parseInstant("2021-07-04T08:00:00 ET")
secondTime = parseInstant("2021-09-06T12:30:00 ET")
thirdTime = parseInstant("2021-12-25T21:15:00 ET")

source = newTable(
    instantCol("DateTimes", firstTime, secondTime, thirdTime)
)

result = source.where("DateTimes = '2021-09-06T12:30:00 ET'")
```

## Related Documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [How to use Deephaven's built-in query language functions](../../../how-to-guides/query-language-functions.md)
- [Query language functions](../query-library/query-language-function-reference.md)
- [`instantCol`](../../table-operations/create/instantCol.md)
- [Strings](./strings.md)
- [`Instant` Javadoc](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html)
- [`ZonedDateTime` Javadoc](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZonedDateTime.html)
