---
title: Work with calendars
---

This guide will show you how to create and use business calendars in Deephaven. In Deephaven, the calendar API is centered around the [`BusinessCalendar`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html) object, which is a calendar with the concept of business and non-business time. These calendars are highly useful in both Groovy code and Deephaven tables.

## Get a calendar

Getting a calendar is simple. The code block below lists the available calendars and grabs the `USNYSE_EXAMPLE` calendar.

```groovy test-set=1 order=:log
import static io.deephaven.time.calendar.Calendars.calendar
import static io.deephaven.time.calendar.Calendars.calendarNames

println calendarNames()
nyseCal = calendar("USNYSE_EXAMPLE")
println nyseCal.getClass()
```

We can see from the output that `nyseCal` is an [`io.deephaven.time.calendar.BusinessCalendar`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html) object. It's Deephaven's business calendar object. A `BusinessCalendar` has many different methods available that can be useful in queries. The sections below explore those uses.

## Business calendar use

### Input data types

`BusinessCalendar` methods accept either strings or Java date-time types ([`Instant`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/Instant.html), [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/ZonedDateTime.html), [`LocalDate`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/LocalDate.html)). See [Time in Deephaven](../conceptual/time-in-deephaven.md#1-built-in-java-functions) for conversion functions.

### Create data

Before we can demonstrate the use of business calendars in queries, we'll need to create a table with some data. The following code block creates about three weeks' worth of date-time data spaced 3 minutes apart.

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
    "NumBizDays = nyseCal.numberBusinessDates('2024-01-01T00:00:00 ET', Timestamp)",
    "NumNonBizDays = nyseCal.numberNonBusinessDates('2024-01-01T00:00:00 ET', Timestamp)"
)
```

The following example shows how to filter data to only business days and business hours. The `source` table is [filtered](./filters.md) twice to create two result tables. The first contains only data that takes place during an NYSE business day, while the second contains only data that takes place during NYSE business hours.

```groovy test-set=1 order=resultBizDays,resultBizHours
resultBizDays = source.where("nyseCal.isBusinessDay(Timestamp)")

resultBizHours = source.where("nyseCal.isBusinessTime(Timestamp)")
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

This example is based on a calendar file in Deephaven's [examples repository](https://github.com/deephaven/examples/tree/main/Calendar), corrected as described below. This guide assumes you save the corrected file in the [/data mount point](../conceptual/docker-data-volumes.md). This hypothetical business is called "Company Y", and the calendar only covers the year 2024.

### The calendar file

A calendar XML file contains top-level information about the calendar itself, business days, business hours, and holidays. While most business calendars have a single business period (e.g., 9am - 5pm), some use two distinct business periods separated by a lunch break. The test calendar file below has two distinct periods: from 8am - 12pm and from 1pm - 5pm. It also specifies a series of holidays over the course of the 2024 calendar year, which includes two half-holidays in which business is open for the first of the two business periods. Calendars typically contain data for more than one year, but this example limits it to 2024 only.

The [`TestCalendar_2024.calendar` file](https://github.com/deephaven/examples/blob/main/Calendar/TestCalendar_2024.calendar) in the examples repository puts both periods in a single `businessTime` element. Deephaven reads only the first `open`/`close` pair in each element, so that copy defines only the 8am - 12pm period. The listing below puts each period in its own `businessTime` element, which defines both. Save the listing as `/data/examples/Calendar/TestCalendar_2024.calendar` in the [Deephaven Docker container](../conceptual/docker-data-volumes.md), replacing the examples-repository copy if you have it. The examples below load the calendar from that path. Expand the file below to see its contents.

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
        <businessTime><open>08:00</open><close>12:00</close></businessTime>
        <businessTime><open>13:00</open><close>17:00</close></businessTime>
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

The second way is through the configuration property `Calendar.userImportPath`, which adds calendars to the built-in set when the server starts. It points to a text file with line-separated locations of calendar files to load. Deephaven loads both this text file and each calendar file it lists as classpath resources, not as filesystem paths. The directory that holds them must be on the server's classpath, and each location is relative to that directory.

Say a `calendars` folder next to your `docker-compose.yml` file contains three calendar files: `MyCalendar.calendar`, `TestCalendar_2024.calendar`, `CrazyCalendar.calendar`. The same folder also contains a text file named `calendar_imports.txt`, which looks as follows:

```txt
/MyCalendar.calendar
/TestCalendar_2024.calendar
/CrazyCalendar.calendar
```

To make Deephaven load this list of calendars automatically upon startup via [`docker compose`](https://docs.docker.com/compose/), mount the folder, add it to the classpath with `EXTRA_CLASSPATH`, and set the property:

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
      - ./calendars:/calendars
    environment:
      - EXTRA_CLASSPATH=/apps/libs/*:/calendars
      - START_OPTS=-Xmx4g -DCalendar.userImportPath=/calendar_imports.txt
```

You can also set `Calendar.userImportPath` in a [configuration file](./configuration/config-file.md) instead of `START_OPTS`. The folder must still be on the classpath.

> [!CAUTION]
> Do not set `Calendar.importPath` to load your own calendars. That property lists the built-in calendars, so overriding it removes `UTC`, `USNYSE_EXAMPLE`, and `USBANK_EXAMPLE`. Because `UTC` is the default value of `Calendar.default`, the default calendar is also lost. Use `Calendar.userImportPath` instead.

### Get an instance of the new calendar

```groovy skip-test
import static io.deephaven.time.calendar.Calendars.calendar

test2024Cal = calendar("TestCalendar_2024")
```

Happy calendar-ing!

## Related documentation

- [Time in Deephaven](../conceptual/time-in-deephaven.md)
- [BusinessCalendar Javadoc](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html)
- [`addCalendarFromFile`](/core/javadoc/io/deephaven/time/calendar/Calendars.html#addCalendarFromFile(java.io.File))
- [`calendar`](/core/javadoc/io/deephaven/time/calendar/Calendars.html#calendar(java.lang.String))
- [`calendarName`](/core/javadoc/io/deephaven/time/calendar/Calendars.html#calendarName())
- [`calendarNames`](/core/javadoc/io/deephaven/time/calendar/Calendars.html#calendarNames())
- [`removeCalendar`](/core/javadoc/io/deephaven/time/calendar/Calendars.html#removeCalendar(java.lang.String))
- [`setCalendar`](/core/javadoc/io/deephaven/time/calendar/Calendars.html#setCalendar(java.lang.String))
- [BusinessCalendar Javadoc](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html)
- [XML Parser Javadoc](/core/javadoc/io/deephaven/time/calendar/BusinessCalendarXMLParser.html)
