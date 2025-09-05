---
title: to_np_busdaycalendar
---

The `to_np_busdaycalendar` method creates a [`numpy.busdaycalendar`](https://numpy.org/doc/stable/reference/generated/numpy.busdaycalendar.html) from a Java [`BusinessCalendar`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html).

> [!NOTE]
> Partial holidays in the business calendar are interpreted as _full_ holidays in the numpy business day calendar.

## Syntax

```python syntax
to_np_busdaycalendar(
  cal: BusinessCalendar,
  include_partial: bool = True
) -> numpy.busdaycalendar
```

## Parameters

<ParamTable>
<Param name="cal" type="BusinessCalendar">

The Java `BusinessCalendar` to convert to a `numpy.busdaycalendar`.

</Param>
<Param name="include_partial" type="bool" optional>

Whether to include partial holidays in the NumPy business day calendar. Default is `True`.

</Param>
</ParamTable>

## Returns

A [`numpy.busdaycalendar`](https://numpy.org/doc/stable/reference/generated/numpy.busdaycalendar.html) from the given Java [`BusinessCalendar`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html).

## Examples

The following example creates a Java [`BusinessCalendar`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html) and converts it to a [`numpy.busdaycalendar`](https://numpy.org/doc/stable/reference/generated/numpy.busdaycalendar.html).

```python order=null
from deephaven.calendar import calendar
from deephaven.numpy import to_np_busdaycalendar

cal = calendar()

result = to_np_busdaycalendar(cal)

print(type(result))
```

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.numpy.html#deephaven.numpy.to_np_busdaycalendar)
