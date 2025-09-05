---
title: calendar_names
---

The `calendar_names` method returns the names of all available calendars.

## Syntax

```
calendar.calendar_names() -> list[str]
```

## Parameters

This method takes no arguments.

## Returns

The names of all available calendars.

## Examples

The following example imports the [calendar module](/core/pydoc/code/deephaven.calendar.html#module-deephaven.calendar) and then prints the result of `calendar_names`.

```python order=:log
from deephaven.calendar import calendar_names

print(calendar_names())
```

## Related documentation

- [`add_calendar`](./add-calendar.md)
- [`calendar`](./calendar.md)
- [`calendar_name`](./calendar-name.md)
- [`remove_calendar`](./remove-calendar.md)
- [`set_calendar`](./set-calendar.md)
- [Pydoc](/core/pydoc/code/deephaven.calendar.html#deephaven.calendar.calendar_names)
