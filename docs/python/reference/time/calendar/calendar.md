---
title: calendar
---

The `calendar` method returns the calendar with the given name. If no name is given, the default calendar is returned. To set the default calendar, use [`set_calendar`](./set-calendar.md).

## Syntax

```python syntax
calendar(name: str = None) -> BusinessCalendar
```

## Parameters

<ParamTable>
<Param name="name" type="str" Optional>

The name of the calendar. If not specified, the default calendar is returned.

</Param>
</ParamTable>

## Returns

A `BusinessCalendar`. Note that the returned type is an [`io.deephaven.time.calendar.BusinessCalendar`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html) Java object that can be used in Python.

## Examples

The following example gets the default calendar.

```python order=null
from deephaven.calendar import calendar

cal = calendar()
```

The following example gets the `USBANK_EXAMPLE` calendar.

```python order=:log
from deephaven.calendar import calendar_names, calendar

print(calendar_names())

usbank_cal = calendar("USBANK_EXAMPLE")
```

The following example uses the `USNYSE_EXAMPLE` calendar in a table operation to see if given dates are business days or not.

```python order=source,result
from deephaven.calendar import calendar
from deephaven import empty_table

nyse_cal = calendar("USNYSE_EXAMPLE")

source = empty_table(10).update("Timestamp = '2024-01-05T00:08:00 ET' + i * 24 * HOUR")
result = source.update(["IsBizDay = nyse_cal.isBusinessDay(Timestamp)"])
```

## Related documentation

- [`add_calendar`](./add-calendar.md)
- [`calendar_name`](./calendar-name.md)
- [`calendar_names`](./calendar-names.md)
- [`remove_calendar`](./remove-calendar.md)
- [`set_calendar`](./set-calendar.md)
- [Pydoc](/core/pydoc/code/deephaven.calendar.html#deephaven.calendar.calendar)
