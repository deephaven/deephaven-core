---
title: formatColumnWhere
---

The `formatColumnWhere` method applies color formatting to a column of the source table, based on user-specified conditions.

## Syntax

```groovy syntax
table.formatColumnWhere(columnName, condition, formula)
```

## Parameters

<ParamTable>
<Param name="columnName" type="String">

The column name.

</Param>
<Param name="condition" type="String">

The condition expression.

</Param>
<Param name="formula" type="String">

Formulas to compute formats for columns or rows in the table; e.g., `"X = Y > 5 ? RED : NO_FORMATTING"`.
For color formats, the result of each formula must be either a color string (such as a hexadecimal RGB color, e.g. `"#040427`"), a [Color](/core/javadoc/io/deephaven/gui/color/Color.html), or a packed `long` representation of the background and foreground color (as returned by `bgfg()` or `bgfga()`).

For decimal formats, the result must be a string, and the formula must be wrapped in the special internal
function `Decimal()`; e.g., ``"X = Decimal(`$#,##0.00`)"``.

</Param>
</ParamTable>

## Returns

A new table applies the specified formatting to the source table.

## Examples

In the following example, we use `formatColumnWhere` to paint the "Timestamp" column red where the value of X is between 2 and 5.

```groovy order=result,tt
tt = timeTable("PT1S").update("X = ii")

result = tt.formatColumnWhere("Timestamp", cond="X > 2 && X < 5", formula="RED")
```

## Related documentation

- [How to apply color formatting to columns in a table](../../../how-to-guides/format-columns.md)
- [How to `select`, `view`, and `update` data](../../../how-to-guides/use-select-view-update.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
