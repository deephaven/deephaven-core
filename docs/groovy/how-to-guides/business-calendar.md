---
title: Work with calendars
---

This guide will show you how to create and use business calendars in Deephaven. It covers the use of Deephaven's calendar API, and the use of a Java [`BusinessCalendar`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html) object in both Groovy and Deephaven tables.

The calendar API is minimal - it allows users to add or remove calendars, as well as get a calendar. The returned calendar is a Java [`BusinessCalendar`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html) object. This object is easier to use in table operations and provides optimal performance.

## Get a calendar

Getting a calendar is simple. The code block below lists the available calendars and grabs the `USNYSE_EXAMPLE` calendar.

```groovy test-set=1 order=:log
import static io.deephaven.time.calendar.Calendars.calendar
import static io.deephaven.time.calendar.Calendars.calendarNames

println calendarNames()
nyse_cal = calendar("USNYSE_EXAMPLE")
println nyse_cal.getClass()
```

We can see from the output that `nyse_cal` is an [`io.deephaven.time.calendar.BusinessCalendar`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html) object. It's Deephaven's Java business calendar object. A `BusinessCalendar` has many different methods available that can be useful in queries. The sections below explore those uses.

## Business calendar use

### Input data types

All of a `BusinessCalendar`'s methods take either strings or Java date-time data types as input. Java's date-time types include:

- [`java.time.Instant`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/Instant.html)
- [`java.time.ZonedDateTime`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/ZonedDateTime.html)
- [`java.time.LocalDate`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/LocalDate.html)

Deephaven has [built-in functionalities for converting to and from these data types](../conceptual/time-in-deephaven.md#1-built-in-java-functions). The methods will be used in examples below.

### Create data

Before we can demonstrate the use of business calendars in queries, we'll need to create a table with some data. The following code block creates a month's worth of date-time data spaced 3 hours apart.

```groovy test-set=1 order=source
// Create sample data
source = emptyTable(10000).update(
    "Timestamp = '2024-01-01T00:00:00 ET' + i * 3 * MINUTE",
    "Value = randomGaussian(0.0, 0.1)"
)
```

### Business days and business time

The following example calculates the number of business days and non-business days (weekends & holidays) between two timestamps.

```groovy test-set=1 order=result
result = source.update(
    "NumBizDays = nyse_cal.numberBusinessDates('2024-01-01T00:00:00 ET', Timestamp)",
    "NumNonBizDays = nyse_cal.numberNonBusinessDates('2024-01-01T00:00:00 ET', Timestamp)"
)
```

The following example shows how to filter data to only business days and business hours. The `source` table is [filtered](./use-filters.md) twice to create two result tables. The first contains only data that takes place during an NYSE business day, while the second contains only data that takes place during NYSE business hours.

```groovy test-set=1 order=result_bizdays,result_bizhours
result_bizdays = source.where("nyse_cal.isBusinessDay(Timestamp)")

result_bizhours = source.where("nyse_cal.isBusinessTime(Timestamp)")
```

These filtered tables can be used for analysis, reporting, or plotting data that occurs only during business days or business hours.

## Create a calendar

Deephaven offers [three pre-built calendars](#get-a-calendar) for use: `UTC`, `USNYSE_EXAMPLE`, and `USBANK_EXAMPLE`.

> [!WARNING]
> The calendars that come with Deephaven are meant to serve as examples. They may not be updated. Deephaven recommends users create their own calendars.

The calendar configuration files can be found [here](https://github.com/deephaven/deephaven-core/tree/main/props/configs/src/main/resources/calendar). They use [XML](https://en.wikipedia.org/wiki/XML) to define the properties of the calendar, which include:

- Valid date range
- Country and time zone
- Description
- Operating hours
- Holidays
- More

Users can build their own calendars by creating a calendar file using the format described in [this Javadoc](/core/javadoc/io/deephaven/time/calendar/BusinessCalendarXMLParser.html). This section goes over an example of using a custom-built calendar for a hypothetical business for the year 2024.

This example uses a calendar file found in Deephaven's [examples repository](https://github.com/deephaven/examples/tree/main/Calendar). This guide assumes you have the file on your local machine in the [/data mount point](../conceptual/docker-data-volumes.md). This hypothetical business is called "Company Y", and the calendar only covers the year 2024.

### The calendar file

A calendar XML file contains top-level information about the calendar itself, business days, business hours, and holidays. While most business calendars have a single business period (e.g., 9am - 5pm), some use two distinct business periods separated by a lunch break. The test calendar file below has two distinct periods: from 8am - 12pm and from 1pm - 5pm. It also specifies a series of holidays over the course of the 2024 calendar year, which includes two half-holidays in which business is open for the first of the two business periods. Calendars typically contain data for more than one year, but this example limits it to 2024 only.

The `TestCalendar_2024.calendar` file can be found [here](https://github.com/deephaven/examples/blob/main/Calendar/TestCalendar_2024.calendar). To see its contents, expand the file below. For the examples that use this calendar, it's placed in the folder `/data/examples/Calendar/` in the [Deephaven Docker container](../conceptual/docker-data-volumes.md).

<details>
<summary>Test calendar for 2024</summary>

```xml
<calendar>
    <name>TestCalendar_2024</name>
    <timeZone>America/New_York</timeZone>
    <language>en</language>
    <country>US</country>
    <firstValidDate>2024-01-01</firstValidDate>
    <lastValidDate>2024-12-31</lastValidDate>
    <description>
        Test calendar for the year 2024.
        This calendar uses two business periods instead of one.
        The periods are separated by a one hour lunch break.
        This calendar file defines standard business hours, weekends, and holidays.
    </description>
        <default>
        <businessTime><open>08:00</open><close>12:00</close><open>13:00</open><close>17:00</close></businessTime>
        <weekend>Saturday</weekend>
        <weekend>Sunday</weekend>
    </default>
    <holiday>
        <date>2024-01-01</date>
    </holiday>
    <holiday>
        <date>2024-01-15</date>
    </holiday>
    <holiday>
        <date>2024-02-19</date>
    </holiday>
    <holiday>
        <date>2024-03-29</date>
    </holiday>
    <holiday>
        <date>2024-04-01</date>
        <businessTime><open>08:00</open><close>12:00</close></businessTime>
    </holiday>
    <holiday>
        <date>2024-05-27</date>
    </holiday>
    <holiday>
        <date>2024-07-04</date>
    </holiday>
    <holiday>
        <date>2024-09-02</date>
    </holiday>
    <holiday>
        <date>2024-10-31</date>
        <businessTime><open>08:00</open><close>12:00</close></businessTime>
    </holiday>
    <holiday>
        <date>2024-11-28</date>
    </holiday>
    <holiday>
        <date>2024-11-29</date>
    </holiday>
    <holiday>
        <date>2024-12-25</date>
    </holiday>
    <holiday>
        <date>2024-12-26</date>
    </holiday>
</calendar>
```

</details>

For more information on formatting custom calendars, see the [XML Parser Javadoc](/core/javadoc/io/deephaven/time/calendar/BusinessCalendarXMLParser.html).

## Use the new calendar

### Add the calendar to the set of available calendars

There are two ways to add a calendar to the set of available calendars.

The first and simplest way to do so is through the calendar API. The following code block shows how it's done using the path to the calendar file just created.

```groovy skip-test
import static io.deephaven.time.calendar.Calendars.addCalendarFromFile

addCalendarFromFile("/data/examples/Calendar/TestCalendar_2024.calendar")
```

The second way is through the configuration property `Calendar.importPath`. This should point to a text file with line-separated locations of any calendar files to load by default. Say your Docker configuration has a `/data/Calendar` folder that contains three calendar files: `MyCalendar.calendar`, `TestCalendar_2024.calendar`, `CrazyCalendar.calendar`. The text file, which we'll name `calendar_imports.txt` and place in the root of your Deephaven deployment, would look as follows:

```txt
/data/Calendar/MyCalendar.calendar
/data/Calendar/TestCalendar_2024.calendar
/data/Calendar/CrazyCalendar.calendar
```

To make Deephaven load this list of calendars automatically upon startup via [`docker compose`](https://docs.docker.com/compose/), you can set the property directly:

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -DCalendar.importPath="/calendar_imports.txt"
```

Alternatively, a [configuration file](./configuration/config-file.md) could be used to set the property.

### Get an instance of the new calendar

```groovy skip-test
import static io.deephaven.time.calendar.Calendars.calendar

test_2024_cal = calendar("TestCalendar_2024")
```

Happy calendar-ing!

## Related documentation

- [Time in Deephaven](../conceptual/time-in-deephaven.md)
- [BusinessCalendar Javadoc](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html)
- [XML Parser Javadoc](/core/javadoc/io/deephaven/time/calendar/BusinessCalendarXMLParser.html)
