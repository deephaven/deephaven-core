---
title: Work with calendars
---

This guide shows you how to create and use business calendars in Deephaven. A business calendar is a calendar that knows which days and hours are business time and which are not, such as weekends, holidays, and hours outside business hours.

This guide covers Deephaven's [Python calendar API](/core/pydoc/code/deephaven.calendar.html#module-deephaven.calendar) and how to use the Java [`BusinessCalendar`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html) object it returns in table operations. Because the calendar is a Java object, query strings can call its methods directly. Calling them directly avoids [Python-Java boundary crossings](../conceptual/python-java-boundary.md) and improves performance.

The Python API is minimal. It lets you:

- List calendars with [`calendar_names`](../reference/time/calendar/calendar-names.md).
- Add and remove calendars with [`add_calendar`](../reference/time/calendar/add-calendar.md) and [`remove_calendar`](../reference/time/calendar/remove-calendar.md).
- Get a calendar with [`calendar`](../reference/time/calendar/calendar.md).
- Get or set the default calendar with [`calendar_name`](../reference/time/calendar/calendar-name.md) and [`set_calendar`](../reference/time/calendar/set-calendar.md). The default calendar is the one that [`calendar`](../reference/time/calendar/calendar.md) returns when you don't pass a name.

This guide uses the functions that list, get, and add calendars. See [Related documentation](#related-documentation) for the others.

## Get a calendar

The code block below uses [`calendar_names`](../reference/time/calendar/calendar-names.md) to list the available calendars and [`calendar`](../reference/time/calendar/calendar.md) to get the `USNYSE_EXAMPLE` calendar.

```python test-set=1 order=:log
from deephaven.calendar import calendar_names, calendar

print(calendar_names())
nyse_cal = calendar("USNYSE_EXAMPLE")
print(type(nyse_cal))
```

Deephaven comes with three calendars: `UTC`, `USNYSE_EXAMPLE`, and `USBANK_EXAMPLE`. The output also confirms that `nyse_cal` is a [`BusinessCalendar`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html). You don't need to be a Java developer to use it. The sections below use its methods in queries.

> [!WARNING]
> The calendars that come with Deephaven are examples, and Deephaven doesn't guarantee that their holidays are complete or current. Create your own calendars for production use.

## Use a calendar in queries

The examples in this section call methods on `nyse_cal`, the calendar from [Get a calendar](#get-a-calendar), and use the `source` table created below.

### Create data

Before you use a calendar in queries, create a table with some data. The following code block creates about three weeks' worth of date-time data spaced 3 minutes apart.

```python test-set=1 order=source
from deephaven import empty_table
from deephaven.updateby import cum_sum

# Create sample data: a random walk sampled every 3 minutes
source = (
    empty_table(10000)
    .update(
        [
            "Timestamp = '2024-01-01T00:00:00 ET' + i * 3 * MINUTE",
            "Value = randomGaussian(0.0, 0.1)",
        ]
    )
    .update_by(cum_sum("Value"))
)
```

### Input data types

Date and time arguments to [`BusinessCalendar`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html) methods are Java date-time types such as [`Instant`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/Instant.html), [`ZonedDateTime`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/ZonedDateTime.html), and [`LocalDate`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/LocalDate.html). Many methods also accept strings. Check the Javadoc for each method's overloads. For example, [`isBusinessTime`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html#isBusinessTime(java.time.Instant)) accepts an `Instant` or a `ZonedDateTime`, but not a string or a `LocalDate`.

To get these types:

- In query strings, write a date-time literal, which Deephaven converts to an [`Instant`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/Instant.html), or use Deephaven's [built-in conversion functions](../conceptual/time-in-deephaven.md#1-built-in-java-functions). The examples below use literals.
- In Python code, use the conversion functions in the [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) module.

### Business days and business time

The following example uses [`numberBusinessDates`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html#numberBusinessDates(java.time.Instant,java.time.Instant)) and [`numberNonBusinessDates`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html#numberNonBusinessDates(java.time.Instant,java.time.Instant)) to calculate the number of business days and non-business days (weekends and holidays) between two timestamps. Both counts include the start and end dates.

```python test-set=1 order=result
result = source.update(
    [
        "NumBizDays = nyse_cal.numberBusinessDates('2024-01-01T00:00:00 ET', Timestamp)",
        "NumNonBizDays = nyse_cal.numberNonBusinessDates('2024-01-01T00:00:00 ET', Timestamp)",
    ]
)
```

The following code block [filters](./filters.md) `source` twice. The first filter uses [`isBusinessDay`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html#isBusinessDay(java.time.Instant)) to keep rows that fall on an NYSE business day. The second uses [`isBusinessTime`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html#isBusinessTime(java.time.Instant)) to keep rows that fall within NYSE business hours.

```python test-set=1 order=result_bizdays,result_bizhours
result_bizdays = source.where(["nyse_cal.isBusinessDay(Timestamp)"])

result_bizhours = source.where(["nyse_cal.isBusinessTime(Timestamp)"])
```

### Plot against business time

To plot data against business time, pass the calendar to the `calendar` argument of a [Deephaven Express](/core/plotly/docs/) plot, such as a [line plot](/core/plotly/docs/line/). The plot's time axis leaves out weekends, holidays, and hours outside business hours, so the plot has no gaps where the market is closed. The following code block plots all of `source` against NYSE business time.

```python test-set=1 order=biz_time_plot
import deephaven.plot.express as dx

biz_time_plot = dx.line(source, x="Timestamp", y="Value", calendar=nyse_cal)
```

The [built-in plotting API](./plotting/api-plotting.md) supports business-time axes through the `business_time` and `calendar` arguments of [`axis`](/core/pydoc/code/deephaven.plot.figure.html#deephaven.plot.figure.Figure.axis). That API is no longer under active development.

## Create a calendar

Deephaven's built-in [calendar files](https://github.com/deephaven/deephaven-core/tree/main/props/configs/src/main/resources/calendar) use [XML](https://en.wikipedia.org/wiki/XML) to define the properties of the calendar, which include:

- Name
- Time zone
- Description
- Valid date range
- Default business hours and weekend days
- Holidays and half-days

Deephaven ignores other elements, such as `<language>` and `<country>`.

To build your own calendar, create a calendar file in the format that the [`BusinessCalendarXMLParser` Javadoc](/core/javadoc/io/deephaven/time/calendar/BusinessCalendarXMLParser.html) describes. The rest of this guide goes over an example of using a custom-built calendar for a hypothetical business for the year 2024. The example is based on a calendar file in Deephaven's [examples repository](https://github.com/deephaven/examples/tree/main/Calendar).

### The calendar file

Most business calendars have a single business period each day, such as 9am to 5pm. Some split the day around a lunch break. The test calendar below covers only 2024 and defines:

- Two business periods: 8am to 12pm and 1pm to 5pm.
- The 2024 holidays, two of which are half-days that are open only for the morning period.

Save the listing as `/data/examples/Calendar/TestCalendar_2024.calendar` in the [Deephaven Docker container](../conceptual/docker-data-volumes.md). The examples below load the calendar from that path.

If you have the examples repository's copy of [`TestCalendar_2024.calendar`](https://github.com/deephaven/examples/blob/main/Calendar/TestCalendar_2024.calendar), replace it. That copy puts both `<open>`/`<close>` pairs in one `<businessTime>` element. Deephaven reads only the first pair in each element, so the copy defines only the morning period. The listing below gives each period its own `<businessTime>` element.

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

## Add and use the new calendar

### Add the calendar to the set of available calendars

Add a calendar from a running session with [`add_calendar`](../reference/time/calendar/add-calendar.md), or [load it when the server starts](#load-calendars-when-the-server-starts).

The following code block uses [`add_calendar`](../reference/time/calendar/add-calendar.md) to load `/data/examples/Calendar/TestCalendar_2024.calendar`, the file you saved in [The calendar file](#the-calendar-file).

```python skip-test
from deephaven.calendar import add_calendar

add_calendar("/data/examples/Calendar/TestCalendar_2024.calendar")
```

### Get an instance of the new calendar

After you add the calendar, get it by the name in its `<name>` element, the same way as a built-in calendar. [`calendar`](../reference/time/calendar/calendar.md) matches the name case-insensitively. [`calendar_names`](../reference/time/calendar/calendar-names.md) lists names in uppercase, such as `TESTCALENDAR_2024`.

```python skip-test
from deephaven.calendar import calendar

test_2024_cal = calendar("TestCalendar_2024")
```

### Use the new calendar in a query

The new calendar works in queries the same way as `nyse_cal` in [Use a calendar in queries](#use-a-calendar-in-queries). The following code block keeps only the rows of the `source` table from [Create data](#create-data) that fall within the test calendar's business hours. Because of the lunch break, the result has no rows between 12pm and 1pm. It also has no rows on the January 1 and January 15 holidays.

```python skip-test
test_bizhours = source.where(["test_2024_cal.isBusinessTime(Timestamp)"])
```

## Load calendars when the server starts

Instead of calling [`add_calendar`](../reference/time/calendar/add-calendar.md) in each session, you can load calendars when the server starts. The configuration property `Calendar.userImportPath` adds calendars to the built-in set when the server starts. Its value is the location of a text file that lists one calendar file location per line. Deephaven loads the text file and each calendar file as classpath resources, not filesystem paths, so:

- The folder that holds the files must be on the server's classpath, the list of directories the Java server searches for resources. The Docker example below adds it with `EXTRA_CLASSPATH`.
- Each location, including the property's value, starts with `/` and is relative to that folder.

Say a `calendars` folder next to your `docker-compose.yml` file contains three calendar files: `MyCalendar.calendar`, `TestCalendar_2024.calendar`, and `CrazyCalendar.calendar`. The same folder also contains a text file named `calendar_imports.txt`, which looks as follows:

```txt
/MyCalendar.calendar
/TestCalendar_2024.calendar
/CrazyCalendar.calendar
```

To load these calendars when the server starts with [Docker Compose](https://docs.docker.com/compose/), mount the folder, add it to the classpath with `EXTRA_CLASSPATH`, and set the property as a JVM argument in `START_OPTS`:

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
> Don't set `Calendar.importPath` to load your own calendars. Use `Calendar.userImportPath` instead. `Calendar.importPath` points to the list of built-in calendars, so overriding it drops `UTC`, `USNYSE_EXAMPLE`, and `USBANK_EXAMPLE` unless your list includes them.
>
> The `Calendar.default` property sets the default calendar, and its value is `UTC` unless you change it. If `UTC` isn't loaded, requesting the default calendar fails with a "No such calendar" error. If you drop `UTC`, also set `Calendar.default` to a calendar you load.

## Related documentation

- [Time in Deephaven](../conceptual/time-in-deephaven.md)
- [`add_calendar`](../reference/time/calendar/add-calendar.md)
- [`calendar`](../reference/time/calendar/calendar.md)
- [`calendar_name`](../reference/time/calendar/calendar-name.md)
- [`calendar_names`](../reference/time/calendar/calendar-names.md)
- [`remove_calendar`](../reference/time/calendar/remove-calendar.md)
- [`set_calendar`](../reference/time/calendar/set-calendar.md)
- [`BusinessCalendar` Javadoc](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html)
- [`BusinessCalendarXMLParser` Javadoc](/core/javadoc/io/deephaven/time/calendar/BusinessCalendarXMLParser.html)
