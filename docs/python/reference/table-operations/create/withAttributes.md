---
title: with_attributes
---

The `with_attributes` method returns a table that is identical to the source table, but with the specified attributes added.

> [!NOTE]
> The table attributes are immutable once defined, and are mostly used internally by the Deephaven engine. For advanced users, certain predefined plugin attributes provide a way to extend Deephaven with custom-built plugins.

## Syntax

```
with_attributes(attrs: Dict[str, Any]) -> Table
```

## Parameters

<ParamTable>
<Param name="attrs>" type="Dict[str, Any]">

A `Dict` of table attribute names and their values.

</Param>
</ParamTable>

## Returns

A table with the specified attributes added.

## Examples

In this example, we create a source table, then create a `result` table with a call to `with_attributes()`. This returns a `result` table that is identical to our source table, but with the attribute we specify (`"MyAttribute"`) added.

```python order=source,result
from deephaven import new_table
from deephaven.column import double_col, string_col

source = new_table(
    [
        double_col("Doubles", [3.1, 5.45, -1.0]),
        string_col("Strings", ["Creating", "New", "Tables"]),
    ]
)

attr_dict = {"MyAttribute": "MyBooleanValue"}

result = source.with_attributes(attr_dict)

print(source.attributes())
print(result.attributes())
```

## Related documentation

- [`time_table`](./timeTable.md)
- [`input_table`](./input-table.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.with_attributes)
