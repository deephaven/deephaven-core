---
title: Drop, move, and rename columns
---

Queries often call for dropping, moving, or renaming columns in a table. This guide covers doing so programmatically.

Like the rest of Deephaven's table operations, these work the same on static and real-time tables. [`drop_columns`](../reference/table-operations/select/drop-columns.md), [`rename_columns`](../reference/table-operations/select/rename-columns.md), and the move methods don't copy any data: the result shares the source table's columns and updates whenever the source does.

The following table lists the methods that do each task:

| Task           | Methods                                                                                                                                                                                                                                                                                                                                                                                                                                |
| -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Drop columns   | [`drop_columns`](../reference/table-operations/select/drop-columns.md), [`select`](../reference/table-operations/select/select.md), [`view`](../reference/table-operations/select/view.md)                                                                                                                                                                                                                                             |
| Move columns   | [`move_columns`](../reference/table-operations/select/move-columns.md), [`move_columns_up`](../reference/table-operations/select/move-columns-up.md), [`move_columns_down`](../reference/table-operations/select/move-columns-down.md), [`select`](../reference/table-operations/select/select.md), [`view`](../reference/table-operations/select/view.md)                                                                             |
| Rename columns | [`rename_columns`](../reference/table-operations/select/rename-columns.md), [`move_columns`](../reference/table-operations/select/move-columns.md), [`move_columns_up`](../reference/table-operations/select/move-columns-up.md), [`move_columns_down`](../reference/table-operations/select/move-columns-down.md), [`select`](../reference/table-operations/select/select.md), [`view`](../reference/table-operations/select/view.md) |

The examples in this guide use the following table:

```python test-set=1
from deephaven import new_table
from deephaven.column import int_col, double_col, string_col

source = new_table(
    [
        string_col("Name", ["Andy", "Claire", "Jane", "Steven"]),
        int_col("StudentID", [1, 2, 3, 4]),
        int_col("TestGrade", [85, 95, 88, 72]),
        int_col("HomeworkGrade", [85, 95, 90, 95]),
        double_col("GPA", [3.0, 4.0, 3.7, 2.8]),
    ]
)
```

## Drop columns from a table

[`drop_columns`](../reference/table-operations/select/drop-columns.md) takes the columns to remove. [`view`](../reference/table-operations/select/view.md) and [`select`](../reference/table-operations/select/select.md) take the columns to keep, so they suit a table where you keep only a few. The two differ in when they compute and store data, as the sections below show. For a side-by-side comparison, see [Choose the right column selection method](./use-select-view-update.md#choose-the-right-column-selection-method).

### `drop_columns`

[`drop_columns`](../reference/table-operations/select/drop-columns.md) removes the columns you name from a table.

```python test-set=1 order=result_drop_one_col,result_drop_two_cols
result_drop_one_col = source.drop_columns(["GPA"])
result_drop_two_cols = source.drop_columns(["TestGrade", "HomeworkGrade"])
```

### `view`

[`view`](../reference/table-operations/select/view.md) keeps only the columns you list and removes the rest of the source table's columns. Columns given by name reuse the source table's data, while columns defined by a [formula](./formulas.md) (such as `Total = TestGrade + HomeworkGrade`) become formula columns.

> [!NOTE]
> A formula column stores only its formula. Deephaven calculates each value when it's accessed.

The following code block uses [`view`](../reference/table-operations/select/view.md) to remove the `StudentID` column from the `source` table.

```python test-set=1
result_no_student_id = source.view(["Name", "TestGrade", "HomeworkGrade", "GPA"])
```

### `select`

Like [`view`](../reference/table-operations/select/view.md), [`select`](../reference/table-operations/select/select.md) keeps only the columns you list and removes the rest. Unlike [`view`](../reference/table-operations/select/view.md), [`select`](../reference/table-operations/select/select.md) stores the data for every column in its result in memory.

> [!IMPORTANT]
> On large tables, [`select`](../reference/table-operations/select/select.md) can use a lot of memory. How [`select`](../reference/table-operations/select/select.md) handles each column depends on where its data comes from:
>
> - A formula column: [`select`](../reference/table-operations/select/select.md) computes the values and stores them.
> - A column whose data comes from files or other sources outside memory: [`select`](../reference/table-operations/select/select.md) stores the data in memory.
> - A column whose type is a Deephaven [vector](../conceptual/arrays-vs-vectors.md): [`select`](../reference/table-operations/select/select.md) always copies it.
> - A column given by name whose data is already in memory: [`select`](../reference/table-operations/select/select.md) reuses that data instead of copying it. The exception is a column that reads its data through a row redirection, such as a column of a sorted refreshing table. [`select`](../reference/table-operations/select/select.md) copies those columns.

The following code block uses [`select`](../reference/table-operations/select/select.md) to remove the `Name` column from the `source` table.

```python test-set=1
result_no_name = source.select(["StudentID", "TestGrade", "HomeworkGrade", "GPA"])
```

## Move columns in a table

Choose a method based on where the columns should go:

- To place columns at a given index, use [`move_columns`](../reference/table-operations/select/move-columns.md).
- To send columns to the left edge, use [`move_columns_up`](../reference/table-operations/select/move-columns-up.md).
- To send columns to the right edge, use [`move_columns_down`](../reference/table-operations/select/move-columns-down.md).
- To list every column in a new order, use [`view`](../reference/table-operations/select/view.md) or [`select`](../reference/table-operations/select/select.md).
- To move and rename a column in one call, pass a rename expression such as `"ID = StudentID"` to any of the move methods. See [Move and rename columns together](#move-and-rename-columns-together).

### `move_columns`

[`move_columns`](../reference/table-operations/select/move-columns.md) moves one or more columns to a specified column index in the resulting table. Column indexes start at 0, the leftmost column.

The following code block uses [`move_columns`](../reference/table-operations/select/move-columns.md) twice: once to move a single column further right, and once to move multiple columns further right.

```python test-set=1 order=result_move_one,result_move_two
result_move_one = source.move_columns(3, ["StudentID"])
result_move_two = source.move_columns(2, ["Name", "StudentID"])
```

In `result_move_one`, `StudentID` lands at index 3, just before `GPA`. In `result_move_two`, `Name` and `StudentID` land at indexes 2 and 3, in the order you list them.

### `move_columns_up`

[`move_columns_up`](../reference/table-operations/select/move-columns-up.md) moves one or more columns to the left edge of the resulting table, starting at index 0.

The following example moves `StudentID` to the leftmost position in the table.

```python test-set=1
result_student_id_first = source.move_columns_up(["StudentID"])
```

### `move_columns_down`

[`move_columns_down`](../reference/table-operations/select/move-columns-down.md) moves one or more columns to the right edge of the resulting table.

The following example moves `TestGrade` and `HomeworkGrade` to the right edge of the table, so they become the last two columns.

```python test-set=1
result_grades_last = source.move_columns_down(["TestGrade", "HomeworkGrade"])
```

### Reorder with `view` and `select`

[`view`](../reference/table-operations/select/view.md) and [`select`](../reference/table-operations/select/select.md) output columns in the order you list them. To reorder a table, list every column in the order you want. The following example puts `GPA` first:

```python test-set=1
result_gpa_first = source.view(
    ["GPA", "Name", "StudentID", "TestGrade", "HomeworkGrade"]
)
```

### Move and rename columns together

[`move_columns`](../reference/table-operations/select/move-columns.md), [`move_columns_up`](../reference/table-operations/select/move-columns-up.md), and [`move_columns_down`](../reference/table-operations/select/move-columns-down.md) also accept rename expressions, such as `"ID = StudentID"`, so you can move and rename a column in one call. A rename expression here follows the same rules as [`rename_columns`](../reference/table-operations/select/rename-columns.md), including the [silent replacement of an existing column](#rename_columns) with the same name. The following example uses [`move_columns_up`](../reference/table-operations/select/move-columns-up.md) to move `StudentID` to the leftmost position and rename it to `ID`:

```python test-set=1
result_id_first = source.move_columns_up(["ID = StudentID"])
```

## Rename columns in a table

[`rename_columns`](../reference/table-operations/select/rename-columns.md) renames columns in place and keeps the rest of the table's columns in their original order. [`view`](../reference/table-operations/select/view.md) and [`select`](../reference/table-operations/select/select.md) can also rename, but they keep only the columns you list. The move methods can rename a column as they move it; see [Move and rename columns together](#move-and-rename-columns-together).

### `rename_columns`

[`rename_columns`](../reference/table-operations/select/rename-columns.md) renames one or more columns in a table. Renaming a column follows the syntax `NewColumnName = OldColumnName`.

> [!IMPORTANT]
> If the new column name matches an existing column that the same call doesn't also rename, [`rename_columns`](../reference/table-operations/select/rename-columns.md) silently replaces the existing column.

The following example renames `StudentID` to `ID` and `GPA` to `GradePointAverage`.

```python test-set=1
result_renamed = source.rename_columns(["ID = StudentID", "GradePointAverage = GPA"])
```

### Rename with `view` and `select`

[`view`](../reference/table-operations/select/view.md) and [`select`](../reference/table-operations/select/select.md) rename with the same `NewColumnName = OldColumnName` syntax as [`rename_columns`](../reference/table-operations/select/rename-columns.md). As we saw when [dropping columns](#drop-columns-from-a-table), they keep only the columns you list, so list every column you want to keep along with the rename.

The following example uses both [`select`](../reference/table-operations/select/select.md) and [`view`](../reference/table-operations/select/view.md) to rename the `Name` column to `StudentName`.

```python test-set=1 order=result_select_renamed,result_view_renamed
result_select_renamed = source.select(
    ["StudentName = Name", "StudentID", "TestGrade", "HomeworkGrade", "GPA"]
)
result_view_renamed = source.view(
    ["StudentName = Name", "StudentID", "TestGrade", "HomeworkGrade", "GPA"]
)
```

## Related documentation

- [Create a new table](./new-and-empty-table.md#new_table)
- [Select and create columns](./use-select-view-update.md)
- [Formulas](./formulas.md)
- [Arrays vs vectors](../conceptual/arrays-vs-vectors.md)
- [`drop_columns`](../reference/table-operations/select/drop-columns.md)
- [`move_columns`](../reference/table-operations/select/move-columns.md)
- [`move_columns_down`](../reference/table-operations/select/move-columns-down.md)
- [`move_columns_up`](../reference/table-operations/select/move-columns-up.md)
- [`rename_columns`](../reference/table-operations/select/rename-columns.md)
- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)
