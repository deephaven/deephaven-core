---
title: to_arrow
---

`to_arrow` converts a Deephaven table to a PyArrow table.

## Syntax

```python syntax
to_arrow(table: Table, cols: list[str] = None) -> pyarrow.lib.Table
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

A Deephaven table.

</Param>
<Param name="cols" type="list[str]" optional>

The columns to convert. Default is `None` which means all columns.

</Param>
</ParamTable>

## Returns

A PyArrow table.

## Examples

The following example converts a Deephaven table to a PyArrow table:

```python order=source
from deephaven import arrow as dhpa
from deephaven import empty_table
import pyarrow as pa

source = empty_table(10).update(["X = i", "Y = sin(X)", "Z = cos(X)"])

pa_source = dhpa.to_arrow(table=source)
```

The following example converts a Deephaven table to a PyArrow table, but only the `X` and `Z` columns:

```python order=source
from deephaven import arrow as dhpa
from deephaven import empty_table
import pyarrow as pa

source = empty_table(10).update(["X = i", "Y = sin(X)", "Z = cos(X)"])

pa_source = dhpa.to_arrow(table=source, cols=["X", "Z"])
```

## Related documentation

- [`read_feather`](./read-feather.md)
- [`to_table`](./to-table.md)
- [Pydoc](/core/pydoc/code/deephaven.arrow.html#deephaven.arrow.to_arrow)
