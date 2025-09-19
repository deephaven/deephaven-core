---
title: time_zone_alias_rm
---

`time_zone_alias_rm` removes a time zone alias.

> [!WARNING]
> Do not use this method in a table operation. This method alters system-level properties, and table operations should _never_ alter system-level properties.

## Syntax

```python syntax
time_zone_alias_rm(alias: str)
```

## Parameters

<ParamTable>
<Param name="alias" type="str">

The alias to remove.

</Param>
</ParamTable>

## Returns

Removes a time zone alias.

## Examples

```python reset
from deephaven.time import time_zone_alias_rm

time_zone_alias_rm("MT")
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.time_zone_alias_rm)
