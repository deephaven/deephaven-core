---
title: format_column_where
---

The `format_column_where` method applies color formatting to a column of the source table, based on user-specified conditions.

## Syntax

```python syntax
table.format_column_where(col: str, cond: str, formula: str) -> Table
```

## Parameters

<ParamTable>
<Param name="col" type="str">

The column name.

</Param>
<Param name="cond" type="str">

The condition expression.

</Param>
<Param name="formula" type="str">

Formulas to compute formats for columns or rows in the table; e.g., `"X = Y > 5 ? RED : NO_FORMATTING"`.
For color formats, the result of each formula must be either a color string (such as a hexadecimal RGB color,
e.g. `"#040427`"), a [Color](/core/javadoc/io/deephaven/gui/color/Color.html), or a packed `long`
representation of the background and foreground color (as returned by `bgfg()` or `bgfga()`).

For decimal formats, the result must be a string, and the formula must be wrapped in the special internal
function `Decimal()`; e.g., ``"X = Decimal(`$#,##0.00`)"``.

</Param>
</ParamTable>

## Returns

A new table that applies the specified formatting to the source table.

## Examples

In the following example, we use `format_column_where` to paint the "X" column red where the value is greater than 2.

```python order=result,tt
from deephaven import time_table

tt = time_table("PT1S").update("X = ii")

result = tt.format_column_where(col="X", cond="X > 2", formula="RED")
```

## Related documentation

- [How to apply color formatting to columns in a table](../../../how-to-guides/format-columns.md)
- [How to select, view, and update data](../../../how-to-guides/use-select-view-update.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.format_column_where)
