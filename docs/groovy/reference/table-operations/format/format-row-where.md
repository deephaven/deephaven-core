---
title: formatRowWhere
---

The `formatRowWhere` method applies color formatting to rows of the source table, based on user-specified conditions.

## Syntax

```groovy syntax
table.formatRowWhere(condition, formula)
```

## Parameters

<ParamTable>
<Param name="condition" type="String">

The condition expression.

</Param>
<Param name="formula" type="String">

Formulas to compute formats for columns or rows in the table; e.g., `"X = Y > 5 ? RED : NO_FORMATTING"`.
For color formats, the result of each formula must be either a color string (such as a hexadecimal RGB color, e.g. `"#040427`"), a [Color](/core/javadoc/io/deephaven/gui/color/Color.html), or a packed `long` representation of the background and foreground color (as returned by `bgfg()` or `bgfga()`).

For decimal formats, the result must be a string, and the formula must be wrapped in the special internal function `Decimal()`; e.g., ``"X = Decimal(`$#,##0.00`)"``.

</Param>
</ParamTable>

## Returns

A new table that applies the specified formatting to the source table.

## Examples

In the following example, we use `formatRowWhere` to paint the table's 4th row red.

```groovy order=result,tt
tt = timeTable("PT1S").update("X = ii")

result = tt.formatRowWhere("X > 2 && X < 4", "RED").view()
```

## Related documentation

- [How to apply color formatting to columns in a table](../../../how-to-guides/format-columns.md)
- [How to `select`, `view`, and `update` data](../../../how-to-guides/use-select-view-update.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
