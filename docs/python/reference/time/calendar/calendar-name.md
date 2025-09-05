---
title: calendar_name
---

The `calendar_name` method returns the name of the default business calendar. Unless explicitly set, the default calendar is `UTC`. The default calendar can be set with [`set_calendar`](./set-calendar.md), or with the `Calendar.default` configuration property.

## Syntax

```
calendar_name() -> str
```

## Parameters

This method takes no arguments.

## Returns

A string.

## Examples

The following example imports the `calendar_name` method from Deephaven's [calendar](/core/pydoc/code/deephaven.calendar.html#module-deephaven.calendar) module and uses it to print the default calendar name.

```python order=:log
from deephaven.calendar import calendar_name

print(calendar_name())
```

## Related documentation

- [`add_calendar`](./add-calendar.md)
- [`calendar`](./calendar.md)
- [`calendar_names`](./calendar-names.md)
- [`remove_calendar`](./remove-calendar.md)
- [`set_calendar`](./set-calendar.md)
- [Pydoc](/core/pydoc/code/deephaven.calendar.html#deephaven.calendar.calendar_name)
