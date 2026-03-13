---
title: Apply color formatting to columns
sidebar_label: Format columns
---

This guide shows you how to apply various color formatting options to the columns in your static or dynamic tables.

> [!CAUTION]
> Formatting creates hidden columns in tables. It's recommended to apply formatting after all other data processing is complete.

## Formatting methods

Deephaven provides several methods for applying color formatting to tables. Here are the most common methods with basic examples.

### `format_columns`

The [`format_columns`](../reference/table-operations/format/format-columns.md) method is the foundational method for applying formatting. It can format specific columns with various conditions.

First, let's create the `students` table that will be used in the following examples:

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

The following query colors the `GPA` column `VIVID_YELLOW`:

```python test-set=1 order=students_gpa_format
students_gpa_format = students.format_columns(["GPA=VIVID_YELLOW"])
```

### `format_column_where`

The [`format_column_where`](../reference/table-operations/format/format-column-where.md) method applies conditional color formatting to a single column. The following example formats the `TestGrade` column when the `GPA` is below 3.0:

```python test-set=1
students_format = students.format_column_where("TestGrade", "GPA<3.0", "DEEP_GREEN")
```

### `format_row_where`

The [`format_row_where`](../reference/table-operations/format/format-row-where.md) method applies conditional color formatting to entire rows. For example, the code below highlights rows where the student's name is "Jane":

```python test-set=1
student_name = students.format_row_where("Name=`Jane`", "BRIGHT_YELLOW")
```

## Colors

Before applying formatting to tables, it's essential to understand the available color options in Deephaven.

### Color types

You can assign colors in Deephaven tables or plots using several different methods:

#### Named color values

There are hundreds of predefined color values available in Deephaven. See the [Named Colors Chart](../assets/how-to/colors.pdf) for all of them. These predefined colors are referred to by their names, which must be typed in capital letters (e.g., `VIVID_YELLOW`, `BRIGHT_GREEN`).

> [!NOTE]
> The `NO_FORMATTING` predefined value indicates that no special format should be applied. This is useful in advanced formats defined with `.format_columns`.

#### HEX (Hexadecimal)

Hexadecimal values are specified with three pairs of values that correspond to red, green, and blue (RRGGBB). Each pair represents a hexadecimal integer between 00 and FF that specifies the intensity of the color. All hex values are preceded with a pound sign, e.g., #0099FF.

Because these values are considered strings, they must be enclosed in quotes. If the HEX color values are to be used within another string (i.e., a string within a string), the name must be enclosed in backticks.

```python test-set=1
# Using HEX color - light sky blue (#87CEFA)
students_hex = students.format_columns("Name = `#87CEFA`")
```

Note: HEX values must be enclosed in backticks when used within a string.

#### RGB and RGBA colors

You can specify RGB and RGBA colors using the `colorRGB` method:

```python test-set=1
# RGB color (integers 0-255) - light sky blue
students_rgb = students.format_columns("Name = colorRGB(135, 206, 250)")
```

#### RGBA (Red, Green, Blue, Alpha)

The RGBA color model is similar to RGB, but also provides the `alpha` parameter to specify transparency. As with RGB, you can specify the numeric values using floats or ints. The numeric ranges for RGB remain the same. The alpha channel can range from 0 to 255 for ints, and 0.0 to 1.0 for floats. The minimum value indicates full transparency, whereas the maximum value indicates fully opaque.

You can convert RGBA color values into color objects in Deephaven by enclosing their values into the argument of the `colorRGB` method.

The syntax follows:

`colorRGB(int r, int g, int b, int a)`

`colorRGB(float r, float g, float b, float a)`

```python test-set=1
students_rgba = students.format_columns("Name = colorRGB(135, 206, 250, 50)")
```

### Background and foreground colors

When formatting table cells, you can control two aspects of the cell's appearance:

- **Background**: The field/fill color of the cell.
- **Foreground**: The color of the text/numbers shown in the cell.

You can use color objects with the following methods to assign colors to the background and/or foreground:

- `bg` or `background` - Set the background to a specific color without changing the foreground.
- `fg` or `foreground` - Set the foreground to a specific color without changing the background.
- `bgfg` or `backgroundForeground` - Set both background and foreground to specific colors.
- `bgfga` or `backgroundForegroundAuto` - Set the background to a specific color while Deephaven automatically chooses a contrasting foreground color.
- `fgo` or `foregroundOverride` - Similar to `fg` but will override the highlight color that appears when users select cells.
- `bgo` or `backgroundOverride` - Similar to `bg` but will override the highlight color that appears when users select cells.

### Color combination best practices

When applying color formatting to your tables, it's essential to consider how background and foreground colors interact. Not all color combinations provide adequate contrast, which can make your data difficult to read or visually unappealing. Here are some guidelines and examples:

- **High contrast combinations** (like black on white or dark blue on light yellow) are generally easier to read.
- **Low contrast combinations** (like light gray on white or dark blue on black) can strain the eyes and make text difficult to distinguish.
- **Complementary colors** may create vibrating effects that cause visual fatigue.
- **Consider accessibility** for users with color vision deficiencies (color blindness).

The following examples demonstrate both effective and problematic color combinations:

```python test-set=1 order=students_orange_bg,students_purple,students_pink,students_navy,students_high_contrast,students_low_contrast,students_vibrating,students_auto_contrast,students_colorblind
# Example 1: Orange background using RGB values (Effective Highlighting)
students_orange_bg = students.format_column_where(
    "Name", "GPA >= 3.5", "bg(colorRGB(255,93,0))"
)

# Example 2: Purple text (foreground) using RGB values (Good for Emphasis)
students_purple = students.format_row_where("true", "fg(colorRGB(102,0,204))")

# Example 3: Hot pink background with yellow text (High Contrast but Potentially Jarring)
students_pink = students.format_columns(
    "Name = bgfg(colorRGB(255,105,80),colorRGB(255,255,0))"
)

# Example 4: Navy blue background with automatically contrasting text (Recommended Approach)
students_navy = students.format_columns("* = bgfga(colorRGB(0,0,128))")

# Example 5: High contrast (Good) - Black text on white background
# Using RGB values for white and black
students_high_contrast = students.format_columns(
    "Name = bgfg(colorRGB(255,255,255), colorRGB(0,0,0))"
)

# Example 6: Low contrast (Poor) - White text on light gray background
# This combination is difficult to read
# Using RGB values for light gray and white
students_low_contrast = students.format_columns(
    "Name = bgfg(colorRGB(211,211,211), colorRGB(255,255,255))"
)

# Example 7: Vibrating colors (Poor) - Red text on blue background
# This creates visual discomfort and fatigue
# Using RGB values for blue and red
students_vibrating = students.format_columns(
    "Name = bgfg(colorRGB(0,0,205), colorRGB(255,0,0))"
)

# Example 8: Automatic contrast (Good) - Let Deephaven choose the text color
# This is generally the safest approach for readability
# Using RGB values for dark green
students_auto_contrast = students.format_columns("* = bgfga(colorRGB(0,100,0))")

# Example 9: Color blind friendly (Good) - Blue and orange have different brightness levels
# This works well for most types of color vision deficiency
students_colorblind = students.format_columns(
    "Name = bgfg(colorRGB(51,153,255), colorRGB(255,128,0))"
)
```

### Tips for selecting effective color combinations

1. **Use the `bgfga` function** when possible, as it automatically selects a contrasting foreground color.
2. **Test your color schemes** with actual data before deploying to production.
3. **Consider your audience** - if your tables will be viewed by people with color vision deficiencies, use patterns that differ in brightness, not just hue.
4. **Be consistent** - use the same color schemes for similar data patterns across your application.
5. **Use colors meaningfully** - colors should help communicate information, not just make the table look interesting.

## Conditional formatting

Conditional formatting allows you to apply colors to cells, columns, or rows based on specific criteria. This can be as simple as highlighting values above a certain threshold or as complex as using multi-part logical expressions.

### Basic conditional formatting

Basic conditional formatting often involves applying a single rule to a column or row.

You can use more complex conditions, like formatting every other row:

```python test-set=1
student_id = students.format_row_where("StudentID % 2 == 0", "VIVID_PURPLE")
```

#### Combining row and column formatting

Row and column formatting can be combined in a single table. When both are applied, column formatting takes precedence over row formatting where they overlap:

```python test-set=1
students_combined = students.format_row_where(
    "StudentID % 2 == 0", "VIVID_PURPLE"
).format_column_where("Name", "GPA > 3.0", "BRIGHT_YELLOW")
```

### Advanced conditional formatting

Since `format_columns` leverages Deephaven's formula infrastructure, you have access to powerful formatting capabilities:

- Complex expressions involving multiple columns.
- User-defined functions and variables.
- All [built-in query language functions](../reference/query-language/query-library/auto-imported/index.md).

#### Ternary expressions

The [format_columns](../reference/table-operations/format/format-columns.md) method supports [ternary expressions](./ternary-if-how-to.md) for more complex conditional formatting:

```python test-set=1
student_tests = students.update("Diff = HomeworkGrade - TestGrade").format_columns(
    "Name = (Diff > 0) ? BRIGHT_GREEN : BRIGHT_RED"
)
```

You can apply more sophisticated conditional formatting with multiple conditions. For example, this code colors the `Name` column based on multiple criteria:

```python test-set=1
students_cond = students.format_columns(
    "Name = TestGrade <= 85 || HomeworkGrade <= 85 ? BRIGHT_YELLOW : GPA < 3 ? LIGHT_RED : NO_FORMATTING"
)
```

This applies the following formatting rules:

- Yellow background when either test grade or homework grade is ≤ 85.
- Light red background when GPA < 3.0.
- No special formatting otherwise.

### Heat Maps

Heat maps are a powerful way to visualize numeric ranges in your data. Deephaven provides built-in heat map functionality through the `heatmap` function:

```syntax
heatmap(<colName>, <minimumValue>, <maximumValue>, <minimumBackgroundColor>, <maximumBackgroundColor>)
```

Where:

- `colName` is the column value to determine the color.
- `minimumValue` and `maximumValue` define the range.
- `minimumBackgroundColor` and `maximumBackgroundColor` are the colors to interpolate between.

For example, this creates a heat map for GPA values ranging from 1.0 to 4.0:

```python test-set=1
students_heat = students.format_columns(
    "GPA = heatmap(GPA, 1, 4, BRIGHT_GREEN, BRIGHT_RED)"
)
```

- When the value is less than or equal to 1.00, `BRIGHT_GREEN` will be used.
- When the value is greater than or equal to 4.00, `BRIGHT_RED` will be used.
- An automatically interpolated color proportionally between `BRIGHT_GREEN` and `BRIGHT_RED` will be used for all other values between 1 and 4.

You can also create foreground heat maps with `heatmapFg()` or `heatmapForeground()` to color the text instead of the background.

### Advanced row formatting

The special column name `__ROWFORMATTED` allows you to format entire rows based on conditions. This is particularly useful for creating banded tables or highlighting specific data patterns:

```python test-set=1
students_row_format = students.format_columns(
    "__ROWFORMATTED = GPA >= 3.5 ? bg(LIGHT_GREEN) : GPA >= 3 ? bg(LIGHT_BLUE) : bg(BRIGHT_YELLOW)"
)
```

This creates a table with:

- Light green rows for high-achieving students (GPA ≥ 3.5).
- Light blue rows for good students (3.0 ≤ GPA < 3.5).
- Light yellow rows for students who may need additional support (GPA < 3.0).

## Related documentation

- [Create a new table](./new-and-empty-table.md#new_table)
- [How to select, view, and update data in tables](./use-select-view-update.md)
- [How to use filters](./use-filters.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [Python classes in query strings](./python-classes.md)
- [`format_columns`](../reference/table-operations/format/format-columns.md)
