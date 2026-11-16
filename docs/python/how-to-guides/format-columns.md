---
title: Apply color formatting to columns
---

This guide shows you how to color cells, columns, and entire rows in your static or dynamic tables. It covers the three formatting methods, the ways to specify a color, and conditional formatting with [ternary expressions](./ternary-if-how-to.md) and heat maps.

> [!CAUTION]
> Formatting adds hidden columns that compute each cell's format. Apply formatting as the last step, after all other data processing is complete.

> [!NOTE]
> You can also [format rows and columns](/core/ui/docs/components/table/#formatting-rows-and-columns) with the [`deephaven.ui`](/core/ui/docs/) Python package.

## Example table

Let's start with the `students` table that every example in this guide uses. Run this code first:

```python test-set=1 order=students
from deephaven import new_table
from deephaven.column import int_col, double_col, string_col

# Create our sample students table that we'll use throughout the examples
students = new_table(
    [
        string_col("Name", ["Andy", "Claire", "Jane", "Steven"]),
        int_col("StudentID", [1, 2, 3, 4]),
        int_col("TestGrade", [85, 95, 88, 72]),
        int_col("HomeworkGrade", [85, 95, 90, 95]),
        double_col("GPA", [3.0, 4.0, 3.7, 2.8]),
    ]
)
```

## Formatting methods

Deephaven provides three methods for applying color formatting to tables: [`format_columns`](../reference/table-operations/format/format-columns.md), [`format_column_where`](../reference/table-operations/format/format-column-where.md), and [`format_row_where`](../reference/table-operations/format/format-row-where.md).

### `format_columns`

The [`format_columns`](../reference/table-operations/format/format-columns.md) method is the foundational formatting method. Each color format is a string of the form `"<ColumnName> = <color expression>"`:

- The left side names the column to color.
- The right side is a [formula](./formulas.md) that Deephaven evaluates for each row to produce a color. It can include a condition, as shown in [Advanced conditional formatting](#advanced-conditional-formatting).

The following query sets the background of the `GPA` column to `VIVID_YELLOW`. Deephaven picks a contrasting text color for you, as [Background and foreground colors](#background-and-foreground-colors) explains:

```python test-set=1 order=students_gpa_format
students_gpa_format = students.format_columns("GPA = VIVID_YELLOW")
```

[`format_columns`](../reference/table-operations/format/format-columns.md) also sets number formats with `Decimal(...)`, as its reference page describes.

### `format_column_where`

The [`format_column_where`](../reference/table-operations/format/format-column-where.md) method applies conditional color formatting to a single column. The following example formats the `TestGrade` column when the `GPA` is below 3.0:

```python test-set=1 order=students_format
students_format = students.format_column_where("TestGrade", "GPA < 3.0", "DEEP_GREEN")
```

`format_column_where(col, cond, formula)` is shorthand for a [ternary](./ternary-if-how-to.md) in [`format_columns`](../reference/table-operations/format/format-columns.md): `format_columns("<col> = (cond) ? formula : NO_FORMATTING")`, where `formula` is a color expression and `NO_FORMATTING` leaves non-matching cells unformatted. The shorthand also converts each branch to the same type, so you can pass any color expression, including [`bg`](../reference/query-language/query-library/auto-imported/gui.md) or [`heatmap`](../reference/query-language/query-library/auto-imported/gui.md).

### `format_row_where`

The [`format_row_where`](../reference/table-operations/format/format-row-where.md) method applies conditional color formatting to entire rows. For example, the code below highlights rows where `Name` is `Jane`:

```python test-set=1 order=student_name
student_name = students.format_row_where("Name = `Jane`", "BRIGHT_YELLOW")
```

`format_row_where(cond, formula)` is the same shorthand with `*` as the column name. The `*` column name stands for the entire row. Use `*` directly in [`format_columns`](../reference/table-operations/format/format-columns.md) when one expression needs to choose among several row colors, as shown in [Advanced row formatting](#advanced-row-formatting).

### Combine row and column formatting

You can combine row and column formatting in one table. Where they overlap, column formatting takes precedence over row formatting:

```python test-set=1 order=students_combined
students_combined = students.format_row_where(
    "StudentID % 2 == 0", "VIVID_PURPLE"
).format_column_where("Name", "GPA > 3.0", "BRIGHT_YELLOW")
```

## Colors

Every color format assigns a color. This section covers the ways to write a color, whether it colors a cell's background or its text, and how to choose readable color combinations.

### Color types

You can specify a color by name, as a hex string, or with a color function such as [`colorRGB`](../reference/query-language/query-library/auto-imported/gui.md) or [`colorHSL`](../reference/query-language/query-library/auto-imported/gui.md).

#### Named color values

Deephaven has hundreds of predefined colors. For the full list, with a swatch and hex value for each, see [Named colors](../reference/table-operations/format/named-colors.md). Refer to a predefined color by its name in capital letters, such as `VIVID_YELLOW` or `BRIGHT_GREEN`.

> [!NOTE]
> The `NO_FORMATTING` predefined value applies no format. Use it as the fallback branch of a [ternary](./ternary-if-how-to.md) in [`format_columns`](../reference/table-operations/format/format-columns.md) to leave non-matching cells or rows unformatted. Both branches of a ternary must have the same type. Named colors, `colorRGB`, and `NO_FORMATTING` are colors, while [`bg`](../reference/query-language/query-library/auto-imported/gui.md), [`fg`](../reference/query-language/query-library/auto-imported/gui.md), [`bgfg`](../reference/query-language/query-library/auto-imported/gui.md), [`bgfga`](../reference/query-language/query-library/auto-imported/gui.md), and [`heatmap`](../reference/query-language/query-library/auto-imported/gui.md) return an encoded format. To pair one of those functions with an unformatted branch, use `bg(NO_FORMATTING)`.

#### Hex colors

A hex color has three pairs of hexadecimal digits for red, green, and blue (`RRGGBB`). Each pair is a value from `00` to `FF` that sets the intensity of that component. A pound sign precedes every hex color, for example `#0099FF`.

A format string is a [query string](./query-string-overview.md), so a hex color inside it needs backticks: ``"Name = `#87CEFA`"``. The same applies to the color argument of [`format_column_where`](../reference/table-operations/format/format-column-where.md) and [`format_row_where`](../reference/table-operations/format/format-row-where.md), which is also a formula: ``"`#87CEFA`"``.

```python test-set=1 order=students_hex
# Using a hex color - light sky blue (#87CEFA)
students_hex = students.format_columns("Name = `#87CEFA`")
```

#### RGB, RGBA, and HSL colors

The [`colorRGB`](../reference/query-language/query-library/auto-imported/gui.md) function creates a color from its red, green, and blue components. Each component is either:

- An int from 0 to 255, as in the following example.
- A float from 0.0 to 1.0, written with an `f` suffix, as in `colorRGB(0.5f, 0.5f, 0.5f)`. A plain decimal such as `0.5` is a double, which `colorRGB` does not accept.

```python test-set=1 order=students_rgb
# RGB color (integers 0-255) - light sky blue
students_rgb = students.format_columns("Name = colorRGB(135, 206, 250)")
```

[`colorRGB`](../reference/query-language/query-library/auto-imported/gui.md) also accepts a fourth `alpha` (transparency) component, but table formatting uses only the red, green, and blue components and ignores alpha. The following example renders the same as the RGB example above:

```python test-set=1 order=students_rgba
students_rgba = students.format_columns("Name = colorRGB(135, 206, 250, 50)")
```

The [`colorHSL`](../reference/query-language/query-library/auto-imported/gui.md) function creates a color from hue (degrees), saturation (percent), and lightness (percent) instead. Its arguments are floats, so pass integers or `f`-suffixed decimals, as in `colorHSL(200, 50.5f, 40)`.

### Background and foreground colors

When formatting table cells, you can control two aspects of the cell's appearance:

- **Background**: The fill color of the cell.
- **Foreground**: The color of the text shown in the cell.

Pass a color to one of the following [auto-imported color functions](../reference/query-language/query-library/auto-imported/gui.md) to set the background, the foreground, or both:

- [`bg`](../reference/query-language/query-library/auto-imported/gui.md) or [`background`](../reference/query-language/query-library/auto-imported/gui.md) — Set the background to a specific color without changing the foreground.
- [`fg`](../reference/query-language/query-library/auto-imported/gui.md) or [`foreground`](../reference/query-language/query-library/auto-imported/gui.md) — Set the foreground to a specific color without changing the background.
- [`bgfg`](../reference/query-language/query-library/auto-imported/gui.md) or [`backgroundForeground`](../reference/query-language/query-library/auto-imported/gui.md) — Set both background and foreground to specific colors.
- [`bgfga`](../reference/query-language/query-library/auto-imported/gui.md) or [`backgroundForegroundAuto`](../reference/query-language/query-library/auto-imported/gui.md) — Set the background to a specific color while Deephaven automatically chooses a contrasting foreground color.
- [`fgo`](../reference/query-language/query-library/auto-imported/gui.md) or [`foregroundOverride`](../reference/query-language/query-library/auto-imported/gui.md) — Set the foreground like [`fg`](../reference/query-language/query-library/auto-imported/gui.md). It also marks the color as an override for the selection highlight, but the Deephaven web UI doesn't use that mark: a selected cell looks the same as with `fg`.
- [`bgo`](../reference/query-language/query-library/auto-imported/gui.md) or [`backgroundOverride`](../reference/query-language/query-library/auto-imported/gui.md) — Set the background like [`bg`](../reference/query-language/query-library/auto-imported/gui.md). It also marks the color as an override for the selection highlight, but the Deephaven web UI doesn't use that mark: a selected cell looks the same as with `bg`.

Assigning a color directly (for example, `"GPA = VIVID_YELLOW"`) sets the background and automatically picks a contrasting foreground, the same as [`bgfga`](../reference/query-language/query-library/auto-imported/gui.md).

The following examples apply [`bg`](../reference/query-language/query-library/auto-imported/gui.md), [`fg`](../reference/query-language/query-library/auto-imported/gui.md), [`bgfg`](../reference/query-language/query-library/auto-imported/gui.md), and [`bgfga`](../reference/query-language/query-library/auto-imported/gui.md):

```python test-set=1 order=students_orange_bg,students_purple,students_pink,students_navy
# bg - orange background, foreground unchanged
students_orange_bg = students.format_column_where(
    "Name", "GPA >= 3.5", "bg(colorRGB(255, 93, 0))"
)

# fg - purple text, background unchanged
students_purple = students.format_row_where("true", "fg(colorRGB(102, 0, 204))")

# bgfg - hot pink background with yellow text
students_pink = students.format_columns(
    "Name = bgfg(colorRGB(255, 105, 180), colorRGB(255, 255, 0))"
)

# bgfga - navy background with an automatically contrasting foreground
students_navy = students.format_columns("* = bgfga(colorRGB(0, 0, 128))")
```

### Color combination best practices

Background and foreground colors interact, and combinations without adequate contrast are hard to read:

- **High contrast combinations** (like black on white or dark blue on light yellow) are generally easier to read.
- **Low contrast combinations** (like light gray on white or dark blue on black) can strain the eyes and make text difficult to distinguish.
- **Highly saturated colors of very different hues** (like red on blue) create a vibrating effect that causes visual fatigue.
- **Colorblind-safe combinations** that differ in brightness, not just hue, stay readable for users with color vision deficiencies.

When you choose colors:

- **Prefer automatic contrast**: use a plain color or [`bgfga`](../reference/query-language/query-library/auto-imported/gui.md), which pick a contrasting foreground for you, instead of [`bgfg`](../reference/query-language/query-library/auto-imported/gui.md) unless you need a specific text color.
- **Be consistent**: use the same color schemes for similar data patterns across your application.
- **Use colors meaningfully**: colors should help communicate information, not just make the table look interesting.
- **Test your color schemes**: try them with actual data before deploying to production.

The following examples demonstrate both effective and problematic color combinations:

```python test-set=1 order=students_high_contrast,students_low_contrast,students_vibrating,students_colorblind
# High contrast (Good) - Black text on white background
# Using RGB values for white and black
students_high_contrast = students.format_columns(
    "Name = bgfg(colorRGB(255, 255, 255), colorRGB(0, 0, 0))"
)

# Low contrast (Poor) - White text on light gray background
# This combination is difficult to read
# Using RGB values for light gray and white
students_low_contrast = students.format_columns(
    "Name = bgfg(colorRGB(211, 211, 211), colorRGB(255, 255, 255))"
)

# Vibrating colors (Poor) - Red text on blue background
# This creates visual discomfort and fatigue
# Using RGB values for blue and red
students_vibrating = students.format_columns(
    "Name = bgfg(colorRGB(0, 0, 205), colorRGB(255, 0, 0))"
)

# Colorblind-safe (Good) - Dark blue and orange differ in brightness, not just hue
# This works well for most types of color vision deficiency
students_colorblind = students.format_columns(
    "Name = bgfg(colorRGB(0, 51, 153), colorRGB(255, 153, 0))"
)
```

## Conditional formatting

Conditional formatting allows you to apply colors to cells, columns, or rows based on specific criteria. This can be as simple as highlighting values above a certain threshold or as complex as using multi-part logical expressions.

### Basic conditional formatting

The [`format_column_where`](../reference/table-operations/format/format-column-where.md) and [`format_row_where`](../reference/table-operations/format/format-row-where.md) examples above each apply a single rule. Conditions can also use arithmetic on column values. For example, this highlights `HomeworkGrade` when it is more than 5 points above `TestGrade`:

```python test-set=1 order=students_improved
students_improved = students.format_column_where(
    "HomeworkGrade", "HomeworkGrade - TestGrade > 5", "BRIGHT_GREEN"
)
```

### Advanced conditional formatting

Because [`format_columns`](../reference/table-operations/format/format-columns.md) evaluates each format as a [query-language formula](./formulas.md), you have access to powerful formatting capabilities. A format can use:

- Complex expressions involving multiple columns.
- User-defined [functions](./python-functions.md) and [variables](./python-variables.md).
- All [built-in query-language functions](../reference/query-language/query-library/auto-imported/index.md).

#### Ternary expressions

The [`format_columns`](../reference/table-operations/format/format-columns.md) method supports [ternary expressions](./ternary-if-how-to.md) for more complex conditional formatting:

```python test-set=1 order=student_tests
student_tests = students.update("Diff = HomeworkGrade - TestGrade").format_columns(
    "Name = (Diff > 0) ? BRIGHT_GREEN : BRIGHT_RED"
)
```

A nested [ternary](./ternary-if-how-to.md) can test several conditions in turn. For example, this code colors the `Name` column based on test grades, homework grades, and `GPA`:

```python test-set=1 order=students_cond
students_cond = students.format_columns(
    "Name = TestGrade < 85 || HomeworkGrade < 85 ? BRIGHT_YELLOW : GPA <= 3.0 ? LIGHT_RED : NO_FORMATTING"
)
```

The nested [ternary](./ternary-if-how-to.md) checks its conditions in order and applies the first one that matches:

1. Yellow background when either `TestGrade` or `HomeworkGrade` is below 85.
2. Otherwise, light red background when `GPA` is 3.0 or lower.
3. Otherwise, no special formatting.

Steven's name is yellow because of his test grade. Andy's grades are both 85, so his name falls through to the second rule and is light red because of his 3.0 `GPA`. Claire's and Jane's names have no formatting.

#### Advanced row formatting

As the [`format_row_where` section](#format_row_where) explains, `*` as the column name in [`format_columns`](../reference/table-operations/format/format-columns.md) formats entire rows. Using `*` directly lets one [ternary expression](./ternary-if-how-to.md) choose among several row colors. Row formatting is useful for highlighting groups of rows, such as performance tiers:

```python test-set=1 order=students_row_format
students_row_format = students.format_columns(
    "* = GPA >= 3.5 ? bg(LIGHT_GREEN) : GPA >= 3 ? bg(LIGHT_BLUE) : bg(BRIGHT_YELLOW)"
)
```

This creates a table with:

- Light green rows for high-achieving students (`GPA` ≥ 3.5).
- Light blue rows for good students (3.0 ≤ `GPA` < 3.5).
- Bright yellow rows for students who may need additional support (`GPA` < 3.0).

### Heat maps

Heat maps are a powerful way to visualize numeric ranges in your data. A heat map colors each cell by where its value falls in a numeric range. Use the [`heatmap`](../reference/query-language/query-library/auto-imported/gui.md) function:

```syntax
heatmap(<colName>, <minimumValue>, <maximumValue>, <minimumBackgroundColor>, <maximumBackgroundColor>)
```

Where:

- `colName` is the column whose value determines the color.
- `minimumValue` and `maximumValue` define the range.
- `minimumBackgroundColor` and `maximumBackgroundColor` are the colors to interpolate between.

For example, this creates a heat map for `GPA` values ranging from 1.0 to 4.0:

```python test-set=1 order=students_heat
students_heat = students.format_columns(
    "GPA = heatmap(GPA, 1, 4, BRIGHT_RED, BRIGHT_GREEN)"
)
```

In this example:

- Values at or below 1.0 get `BRIGHT_RED`.
- Values at or above 4.0 get `BRIGHT_GREEN`.
- Values in between get a color interpolated proportionally between `BRIGHT_RED` and `BRIGHT_GREEN`.

You can also create foreground heat maps with [`heatmapFg` or `heatmapForeground`](../reference/query-language/query-library/auto-imported/gui.md) to color the text instead of the background.

## Related documentation

- [Create a new table](./new-and-empty-table.md#new_table)
- [How to select, view, and update data in tables](./use-select-view-update.md)
- [How to use filters](./use-filters.md)
- [Query strings](./query-string-overview.md)
- [Formulas in query strings](./formulas.md)
- [Ternary-if expressions](./ternary-if-how-to.md)
- [Auto-imported color functions](../reference/query-language/query-library/auto-imported/gui.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [`format_columns`](../reference/table-operations/format/format-columns.md)
- [`format_column_where`](../reference/table-operations/format/format-column-where.md)
- [`format_row_where`](../reference/table-operations/format/format-row-where.md)
- [Format rows and columns with `deephaven.ui`](/core/ui/docs/components/table/#formatting-rows-and-columns)
