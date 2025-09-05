---
title: to_table
---

`to_table` converts a PyArrow table to a Deephaven table.

## Syntax

```python syntax
to_table(pa_table: pyarrow.Table, cols: list[str] = None) -> Table
```

## Parameters

<ParamTable>
<Param name="pa_table" type="pyarrow.Table">

A PyArrow table.

</Param>
<Param name="cols" type="list[str]" optional>

The columns to convert. Default is `None`, which means all columns.

</Param>
</ParamTable>

## Returns

A Deephaven Table.

## Examples

The following example converts a PyArrow table to a Deephaven table:

```python order=source
from deephaven import arrow as dhpa
import pyarrow as pa

n_legs = pa.array([2, 4, 5, 100])
animals = pa.array(["Flamingo", "Horse", "Brittle stars", "Centipede"])
sizes = pa.array(["Medium", "Big", "Small", "Small"])
source_pa = pa.table({"n_legs": n_legs, "animals": animals, "sizes": sizes})

source = dhpa.to_table(pa_table=source_pa)
```

The following example converts a PyArrow table to a Deephaven table, but only the `animals` and `n_legs` columns:

```python order=source
from deephaven import arrow as dhpa
import pyarrow as pa

n_legs = pa.array([2, 4, 5, 100])
animals = pa.array(["Flamingo", "Horse", "Brittle stars", "Centipede"])
sizes = pa.array(["Medium", "Big", "Small", "Small"])
source_pa = pa.table({"n_legs": n_legs, "animals": animals, "sizes": sizes})

source = dhpa.to_table(pa_table=source_pa, cols=["animals", "n_legs"])
```

## Related documentation

- [`read_feather`](./read-feather.md)
- [`to_arrow`](./to-arrow.md)
- [Pydoc](/core/pydoc/code/deephaven.arrow.html#deephaven.arrow.to_table)
