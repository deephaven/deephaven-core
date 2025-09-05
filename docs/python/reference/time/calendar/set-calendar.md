---
title: set_calendar
---

The `set_calendar` method sets the default calendar. The default calendar can also be set with the `Calendar.default` configuration property.

## Syntax

```python syntax
set_calendar(name: str) -> None
```

## Parameters

<ParamTable>
<Param name="name" type="str">

The name of the calendar.

</Param>
</ParamTable>

## Returns

None.

## Examples

The following example sets the default calendar to `USNYSE_EXAMPLE`.

```python order=null reset
from deephaven.calendar import set_calendar

set_calendar("USNYSE_EXAMPLE")
```

The default calendar can also be set via the `Calendar.default` [configuration property](../../../how-to-guides/configuration/docker-application.md). The following `docker-compose.yml` file adds this configuration property to Deephaven's [base Python YAML file](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/base/docker-compose.yml):

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server:${VERSION:-latest}
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -DCalendar.default="USNYSE_EXAMPLE"
```

For [pip-installed Deephaven](../../../getting-started/pip-install.md), the equivalent is as follows:

```python skip-test
from deephaven_server import Server

s = Server(
    port=10000, jvm_args=["-Xmx4g", "-DCalendar.default=`USNYSE_EXAMPLE`"]
).start()
```

## Related documentation

- [`add_calendar`](./add-calendar.md)
- [`calendar`](./calendar.md)
- [`calendar_name`](./calendar-name.md)
- [`calendar_names`](./calendar-names.md)
- [`remove_calendar`](./remove-calendar.md)
- [Pydoc](/core/pydoc/code/deephaven.calendar.html#deephaven.calendar.set_calendar)
