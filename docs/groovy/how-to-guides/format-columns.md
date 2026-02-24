---
title: Apply color formatting to columns
sidebar_label: Format columns
---

This guide shows you how to apply various color formatting options to the columns in your static or dynamic tables.

> [!CAUTION]
> Formatting creates hidden columns in tables. It's recommended to apply formatting after all other data processing is complete.

## Format an entire column

Color formatting can be applied to the contents of an entire column using the `formatColumns` method:

[`.formatColumns("columnName=<colorObject>")`](../reference/table-operations/format/format-columns.md)

- `columnName` is the name of the column to format.
- `<colorObject>` is any of the [valid color objects](../assets/how-to/colors.pdf) available in Deephaven.

The following query will apply the color `VIVID_YELLOW` to all cells in the `GPA` column:

```groovy test-set=1
students = newTable(
    stringCol("Name", "Andy", "Claire", "Jane", "Steven"),
    intCol("StudentID", 1, 2, 3, 4),
    intCol("TestGrade", 85, 95, 88, 72),
    intCol("HomeworkGrade", 85, 95, 90, 95),
    doubleCol("GPA", 3.0, 4.0, 3.7, 2.8)
).formatColumns("GPA=VIVID_YELLOW")
```

## Format certain rows or columns

### Columns

You can apply conditional formatting to only certain rows and/or columns.

The following method formats cells in the named column when a specified condition exists:

[`.formatColumnWhere("columnName", "<condition>", "colorValue")`](../reference/table-operations/format/format-column-where.md)

For example, the following query applies the color `DEEP_GREEN` to the `TestGrade` column when the value in the `GPA` column of the same row is less than `3.0`.

```groovy test-set=1
students_format=students.formatColumnWhere("TestGrade", "GPA<3.0", "DEEP_GREEN")
```

[Ternary statements](./ternary-if-how-to.md) can also be used to color cells based on conditional statements. For example, the following query colors cells in the `Name` column `BRIGHT_GREEN` if the value in the `Diff` column is positive, and `BRIGHT_RED` if otherwise:

```groovy test-set=1
student_tests =  students_format
    .update("Diff = HomeworkGrade - TestGrade")
    .formatColumns("Name = (Diff > 0) ? BRIGHT_GREEN : BRIGHT_RED")
```

### Rows

The [`formatRowWhere`](../reference/table-operations/format/format-row-where.md) method formats entire rows when a specified condition exists:

`.formatRowWhere("<condition>", "<colorValue>")`

The following query applies the color `PALE_BLUE` to any row when the value in the `Diff` column is greater than 0.

```groovy test-set=1
student_all_row =  student_tests
    .formatRowWhere("Diff > 0"," PALE_BLUE")
```

The following query colors the entire row `BRIGHT_YELLOW` if the `Sym` column is equal to the string "AAPL":

```groovy test-set=1
student_name = student_all_row.formatRowWhere("Name=`Jane`", "BRIGHT_YELLOW")
```

The following colors all the cells in every other row `VIVID_PURPLE`:

```groovy test-set=1
student_id = student_name.formatRowWhere("StudentID % 2 == 0", "VIVID_PURPLE")
```

### Row formatting with highlighted columns

Row and column formatting can also be combined in a table. All columns will use the row format by default, but if
a column format is specified, it will override the row format. For example:

The following query colors all cells in every other row `VIVID_PURPLE`, and colors the cells in column C `BRIGHT_YELLOW` in every odd row.

```groovy test-set=1
students_combined = student_tests.formatRowWhere("StudentID % 2 == 0", "VIVID_PURPLE")
    .formatColumnWhere("Name", "Diff > 0", "BRIGHT_YELLOW")
```

## Advanced formatting

More advanced formats, including conditional formatting, can be achieved using
the [`.formatColumns`](../reference/table-operations/format/format-columns.md) method. In fact,
both [`.formatRowWhere`](../reference/table-operations/format/format-row-where.md) and [`.formatColumnWhere`](../reference/table-operations/format/format-column-where.md) are wrappers for [`.formatColumns`](../reference/table-operations/format/format-columns.md).

The [`.formatColumns`](../reference/table-operations/format/format-columns.md) method works similarly to [`updateView`](../reference/table-operations/select/update-view.md) — however, the result of the
formula is used to determine the format for an existing column, rather than to add a new column to the table.
The result of a formula passed to [`.formatColumns`](../reference/table-operations/format/format-columns.md) must be either a color string (such as a hexadecimal RGB color,
e.g. `"#040427`"), a [Color](/core/javadoc/io/deephaven/gui/color/Color.html), or a packed `long`
representation of the background and foreground color (as returned by [`bgfg()` or `bgfga()`](#assign-colors-to-backgrounds-and-foregrounds)).

Since `.formatColumns` leverages Deephaven's existing formula infrastructure, the full power of the Deephaven query
engine is available to format formulas in `.formatColumns`. This allows column formats to utilize multiple
Deephaven columns, user-defined functions and constants (such as custom colors or hashmaps of values to colors),
and the flexibility of the query language
(including built-in [query language functions](../reference/query-language/query-library/query-language-function-reference.md)).

### Heat Maps

Color-based formatting can also be used to create heat maps in Deephaven tables:

`heatmap(<colName>, <minimumValue>, <maximumValue>, <minimumBackgroundColor>, <maximumBackgroundColor>)`

The following query will apply color to the `GPA` column as follows:

- When the value is less than or equal to 1.00, `BRIGHT_GREEN` will be used,
- When the value is greater than or equal to 4.00, `BRIGHT_RED` will be used, and
- An automatically interpolated color proportionally between `BRIGHT_GREEN` and `BRIGHT_RED` will be used for all other values between 1 and 4.

```groovy test-set=1
students_heat = students.formatColumns("GPA = heatmap(GPA, 1, 4, BRIGHT_GREEN, BRIGHT_RED)")
```

Options are also available for `heatmapFg()` and `heatmapForeground()`. When either of these methods is used, the heatmap color pair listed in the argument is applied only to the foreground.

### Conditional formatting

Advanced conditional formats can be applied via `formatColumns()`, such as different colors for different values. The
following query will use a [terrnary if](./ternary-if-how-to.md) to color the `Name` column as follows:

- When the `TestGrade` or `HomeworkGrade` is less than 85, `BRIGHT_YELLOW` will be used.
- When the `GPA` is less than 3.0, `LIGHT_RED` will be used.
- Otherwise, no formatting will be applied.

```groovy test-set=1
students_cond = students.formatColumns("Name = TestGrade < 85 || HomeworkGrade < 85 ? BRIGHT_YELLOW : GPA < 3 ? LIGHT_RED : NO_FORMATTING")
```

### Advanced row formatting

The default color for a row can also be specified in a `.formatColumns()` formula by using the special variable
`__ROWFORMATTED` as the destination column name. The following query will color the entire row, using different
colors depending on the value of the `GPA` column.

```groovy test-set=1
students_cond = students.formatColumns("__ROWFORMATTED = GPA >= 3.5 ? LIGHT_GREEN : GPA >= 3 ? LIGHT_BLUE : BRIGHT_YELLOW")
```

## Assign colors to backgrounds and foregrounds

The field of a cell is the background. The text/numbers showing in the cell is the foreground. Color objects can then be used with the following methods to assign individual color values or combinations to the background and/or foreground:

- `bg()` or `background()` - These methods set the background to a specific color, but do not apply any foreground color.
- `fg()` or `foreground()` - These methods set the foreground to a specific color, but do not apply any background color.
- `bgfg()` or `backgroundForeground()` - These methods set both the background and foreground to specific values.
- `bgfga()` or `backgroundForegroundAuto()` - These methods set the background to a specific color. Deephaven automatically chooses a contrasting foreground color.
- `fgo()` or `foregroundOverride()` - These methods are similar to `fg()` or `foreground()`. However, when either of these methods are used, the color selected will override the highlight color that is automatically assigned when the user highlights the cell or group of cells in the Deephaven console.
- `bgo()` or `backgroundOverride()` - These methods are similar to `bg()` or `background()`. However, when either of these methods are used, the color selected will override the highlight color that is automatically assigned when the user highlights the cell or group of cells in the Deephaven console.

> [!CAUTION]
> Overriding the foreground or background colors may make the highlighted content difficult to read. Care in use is suggested.

The following query generates a table with an orange background using RGB values:

```groovy test-set=1
students_combined = student_tests
    .formatColumnWhere("Name", "Diff > 0", "bg(colorRGB(255,93,0))")
```

The following query generates a table with a purple foreground using RGB values:

```groovy test-set=1
students_purple = students.formatRowWhere("true", "fg(colorRGB(102,0,204))")
```

The following query will color the `Name` column with a hot pink background and a yellow foreground.

```groovy test-set=1
students_pink = students.formatColumns("Name = bgfg(colorRGB(255,105,80),colorRGB(255,255,0))")
```

The following query generates a table with a navy blue background (defined by `colorRGB(0,0,128)`) and automatically
selects a contrasting foreground (using `bgfga()`).

```groovy test-set=1
students_navy = students.formatColumns("* = bgfga(colorRGB(0,0,128))")
```

## Appendix: Assigning colors

Colors can be assigned in Deephaven tables or plots using strings or by using color objects.

### Named color values

There are 280 predefined color values available in Deephaven.

> [!NOTE]
> See:
> [Named Colors Chart](../assets/how-to/colors.pdf)

These predefined colors are referred to by their names, which must be typed in capital letters.

> [!NOTE]
> The `NO_FORMATTING` predefined value indicates that no special format should be applied. This is useful in advanced
> formats defined with `.formatColumns()`.

### HEX (Hexadecimal)

Hexadecimal values are specified with three values that correspond to RRGGBB. Each value [RR (red), GG (green) and BB (blue)] are hexadecimal integers between 00 and FF, and they specify the intensity of the color. All Hex values are preceded with a pound sign, e.g., #0099FF.

Because these values are considered strings, they must be enclosed in quotes. If the HEX color values are to be used within another string (i.e., a string within a string), the name must be enclosed in backticks.

```groovy test-set=1
students_hex = students.formatColumns("Name = `#87CEFA`")
```

#### RGB (Red, Green, Blue)

RGB values are represented numerically with comma-separated values for red, green and blue respectively, with each value expressed as integers in the range 0-255 or floats in the range 0-1.

RGB color values can be converted into color objects in Deephaven by enclosing their values into the argument of the `colorRGB` method.

The syntax follows:

`colorRGB(int r, int g, int b)`

`colorRGB(float r, float g, float b)`

```groovy test-set=1
students_rgb = students.formatColumns("Name = colorRGB(135, 206, 250)")
```

#### RGBA (Red, Green, Blue, Alpha)

The RGBA color model is based on RGB. However, RGBA provides an option for a fourth value to specify alpha (transparency). As with RGB, the numeric values can be specified using floats or ints. The numeric ranges for RGB remain the same. The alpha channel can range from 0 (fully transparent) to 255 (fully opaque) for ints, and 0.0 to 1.0 for floats.

RGBA color values can be converted into color objects in Deephaven by enclosing their values into the argument of the `colorRGB` method.

The syntax follows:

`colorRGB(int r, int g, int b, int a)`

`colorRGB(float r, float g, float b, float a)`

```groovy test-set=1
students_rgba = students.formatColumns("Name = colorRGB(135, 206, 250, 50)")
```

## Related documentation

- [Create a new table](./new-and-empty-table.md#newtable)
- [How to select, view, and update data in tables](./use-select-view-update.md)
- [How to use filters](./filters.md)
- [How to use variables and functions in query strings](./query-scope.md)
- [`formatColumns()`](../reference/table-operations/format/format-columns.md)
- [`formatColumnWhere()`](../reference/table-operations/format/format-column-where.md)
- [`formatRowWhere()`](../reference/table-operations/format/format-row-where.md)
