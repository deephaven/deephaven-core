---
title: add_calendar
---

The `add_calendar` method adds a new business calendar to the set of available options.

## Syntax

```python syntax
add_calendar(cal: Union[BusinessCalendar, str]) -> None
```

## Parameters

<ParamTable>
<Param name="cal" type="Union[BusinessCalendar, str]">

A `BusinessCalendar` or path to a business calendar file. Supported file format is XML. See [here](https://raw.githubusercontent.com/deephaven/examples/main/Calendar/TestCalendar_2024.calendar) for an example of a calendar XML file, and [here](https://github.com/deephaven/deephaven-core/blob/main/engine/time/src/main/java/io/deephaven/time/calendar/BusinessCalendarXMLParser.java#L32) for more information on calendar XML file format.

</Param>
</ParamTable>

## Returns

None.

## Example

The following code block adds a new business calendar, `TestCalendar_2024.calendar`, to the set of available business calendars. This calendar can be found in the [examples repository](https://github.com/deephaven/examples/tree/main/Calendar).

```python skip-test
from deephaven.calendar import add_calendar, calendar

add_calendar("/data/examples/Calendar/TestCalendar_2024.calendar")

test_2024_cal = calendar("TestCalendar_2024")
```

Business calendar files should be formatted as:

```xml
<calendar>
 *     <name>CalendarName</name>
 *     <!-- Optional description -->
 *     <description>Example calendar</description>
 *     <timeZone>America/Chicago</timeZone>
 *     <default>
 *          <businessTime><open>08:30</open><close>16:30</close></businessTime>
 *          <weekend>Saturday</weekend>
 *          <weekend>Sunday</weekend>
 *      </default>
 *      <!-- Optional firstValidDate.  Defaults to the first holiday. -->
 *      <firstValidDate>1999-01-01</firstValidDate>
 *      <!-- Optional lastValidDate.  Defaults to the first holiday. -->
 *      <lastValidDate>2003-12-31</lastValidDate>
 *      <holiday>
 *          <date>1999-01-01</date>
 *      </holiday>
 *      <holiday>
 *          <date>2002-07-05</date>
 *          <businessTime>
 *              <open>09:30</open>
 *              <close>13:00</close>
 *          </businessTime>
 *      </holiday>
 * </calendar>
```

## Related documentation

- [`calendar`](./calendar.md)
- [`calendar_name`](./calendar-name.md)
- [`calendar_names`](./calendar-names.md)
- [`remove_calendar`](./remove-calendar.md)
- [`set_calendar`](./set-calendar.md)
- [Pydoc](/core/pydoc/code/deephaven.calendar.html#deephaven.calendar.add_calendar)
