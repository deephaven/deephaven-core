---
title: remove_calendar
---

The `remove_calendar` method removes the specified calendar from the available calendars.

## Syntax

```python syntax
remove_calendar(name: str) -> None
```

## Parameters

<ParamTable>
<Param name="name" type="str">

The name of the calendar to remove.

</Param>
</ParamTable>

## Returns

None.

## Example

The following example removes the `USBANK_EXAMPLE` calendar from the list of usable calendars.

```python order=null reset
from deephaven.calendar import remove_calendar

remove_calendar("USBANK_EXAMPLE")
```

## Related documentation

- [`add_calendar`](./add-calendar.md)
- [`calendar`](./calendar.md)
- [`calendar_name`](./calendar-name.md)
- [`calendar_names`](./calendar-names.md)
- [`set_calendar`](./set-calendar.md)
- [Pydoc](/core/pydoc/code/deephaven.calendar.html#deephaven.calendar.remove_calendar)
