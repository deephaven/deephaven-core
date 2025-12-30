---
title: read_feather
---

`read_feather` reads a feather file into a Deephaven table.

## Syntax

```python syntax
read_feather(path: str) -> Table
```

## Parameters

<ParamTable>
<Param name="path" type="str">

The file path.

</Param>
</ParamTable>

## Returns

A Deephaven table.

## Example

The following example reads a feather file into a Deephaven table:

```python order=source
from deephaven import arrow as dharrow

source = dharrow.read_feather("/data/examples/Iris/feather/Iris.feather")
```

## Related documentation

- [`to_arrow`](./to-arrow.md)
- [`to_table`](./to-table.md)
- [Pydoc](/core/pydoc/code/deephaven.arrow.html#deephaven.arrow.read_feather)
