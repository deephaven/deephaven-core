---
title: format_columns
---

The `format_columns` method creates a new table containing a new formula column defining a column format for each argument. It is a special case of [`update_view`](../select/update-view.md) that creates columns that hold format information instead of regular data.

Since `format_columns` is implemented using `update_view`, the new columns are not stored in memory, and instead a formula is stored that is used to determine the format for each cell/row when necessary.

> [!NOTE]
> [Formatting columns](/core/ui/docs/components/table/#formatting-rows-and-columns) can also be accomplished using the [`deephaven.ui`](/core/ui/docs/) Python package.

## Syntax

```python syntax
table.format_columns(column_formats: Union[str, list[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="column_formats" type="Union[str, list[str]]">

Formulas to compute formats for columns or rows in the table; e.g., `"X = Y > 5 ? RED : NO_FORMATTING"`.

For color formats, the result of each formula must be either a color string (such as a hexadecimal RGB color, e.g. `"#040427`"), a [Color](/core/javadoc/io/deephaven/gui/color/Color.html), or a packed `long` representation of the background and foreground color (as returned by `bgfg()` or `bgfga()`).

For decimal formats, the result must be a string, and the formula must be wrapped in the special internal
function `Decimal()`; e.g., ``"X = Decimal(`$#,##0.00`)"``.

</Param>
</ParamTable>

## Returns

A new table that includes all the original columns from the source table and the newly defined format columns. The format columns are identified internally by the query engine by appending a suffix to the column name.

## Examples

In the following example, column `A` is formatted with a color depending on the value of column `B`, and column `C` is specified to display values as a percentage using the decimal format pattern `0.00%`.

```python order=source,result
from deephaven import new_table
from deephaven.column import int_col, double_col, string_col

source = new_table(
    [
        string_col("A", ["The", "At", "Is", "On"]),
        int_col("B", [1, 2, 3, 4]),
        double_col("C", [0.5, 0.66, 0.777, 0.888]),
    ]
)

result = source.format_columns(
    ["A = B > 2 ? BLUE : NO_FORMATTING", "C = Decimal(`0.00%`)"]
)
```

## Related documentation

- [How to apply color formatting to columns in a table](../../../how-to-guides/format-columns.md)
- [How to `select`, `view`, and `update` data](../../../how-to-guides/use-select-view-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#formatColumns(java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.format_columns)
