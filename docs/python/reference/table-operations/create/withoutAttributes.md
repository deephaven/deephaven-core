---
title: without_attributes
---

The `without_attributes` method returns a table that is identical to the source table, but with the specified attributes removed.

## Syntax

```
without_attributes(attrs: Union[str, Sequence[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="attrs>" type="Union[str, Sequence[str]]">

The names of the attributes to remove.

</Param>
</ParamTable>

## Returns

A table with the specified attributes removed.

## Examples

The following example removes the `InputTable` attribute from an [Input Table](./input-table.md), which turns it back into a regular table.

```python order=source,result
from deephaven import empty_table, input_table
from deephaven import dtypes as dht

# create a table
init_table = empty_table(5).update(["X = i", "Y = (double)(2 * i)"])

my_col_defs = {"X": dht.int32, "Y": dht.double}

source = input_table(col_defs=my_col_defs)

source.add(init_table)

# see what attributes the input table has
print(source.attributes())

# remove attribute
result = source.without_attributes("InputTable")

# prove that "InputTable" attribute has been removed successfully
print(result.attributes())
```

## Related documentation

- [`time_table`](./timeTable.md)
- [`input_table`](./input-table.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.without_attributes)
