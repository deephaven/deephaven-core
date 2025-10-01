---
title: Drop, move, and rename columns
---

Queries often call for dropping, moving, or renaming columns in a table. This guide covers doing so programmatically.

The following methods drop columns from a table:

- [`drop_columns`](../reference/table-operations/select/drop-columns.md)
- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)

The following methods move columns in a table:

- [`move_columns_up`](../reference/table-operations/select/move-columns-up.md)
- [`move_columns_down`](../reference/table-operations/select/move-columns-down.md)
- [`move_columns`](../reference/table-operations/select/move-columns.md)
- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)

The following methods rename columns in a table:

- [`rename_columns`](../reference/table-operations/select/rename-columns.md)
- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)

The examples in this guide will use the following table:

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

## Remove columns from a table

### `drop_columns`

[`drop_columns`](../reference/table-operations/select/drop-columns.md) removes one or more columns from a table based on the column names it's given.

```python test-set=1 order=result_drop_one_col,result_drop_two_cols
result_drop_one_col = source.drop_columns(["GPA"])
result_drop_two_cols = source.drop_columns(["TestGrade", "HomeworkGrade"])
```

### `view`

[`view`](../reference/table-operations/select/view.md) removes any columns not given as input that are present in the source table. If [`view`](../reference/table-operations/select/view.md) is not given any input, it returns a table the same as the source table but with all formula columns.

> [!IMPORTANT]
> Formula columns initially store only the formulas used to create columnar data. Values are calculated on the fly as needed.

The following code block uses [`view`](../reference/table-operations/select/view.md) to remove the `StudentID` column from the `source` table.

```python test-set=1
result_no_student_id = source.view(["Name", "TestGrade", "HomeworkGrade", "GPA"])
```

### `select`

[`select`](../reference/table-operations/select/select.md) is similar to [`view`](../reference/table-operations/select/view.md) in that it removes any columns not given as input that are present in the source table. However, unlike [`view`](../reference/table-operations/select/view.md),[`select`](../reference/table-operations/select/select.md) creates in-memory columns.

> [!IMPORTANT]
> In-memory columns store _all_ of their data in memory. This includes columns of any type, such as formula or memoized columns. Be careful when using [`select`](../reference/table-operations/select/select.md) with large tables, as it can consume a lot of memory.

The following code block uses [`select`](../reference/table-operations/select/select.md) to remove the `Name` column from the `source` table.

```python test-set=1
result_no_name = source.select(["StudentID", "TestGrade", "HomeworkGrade", "GPA"])
```

## Move columns in a table

### `move_columns`

[`move_columns`](../reference/table-operations/select/move-columns.md) moves one or more columns to a specified column index in the resulting table.

The following code block uses [`move_columns`](../reference/table-operations/select/move-columns.md) twice: once to move a single column further right, and once to move multiple columns further right.

```python test-set=1 order=result_move_one,result_move_two
result_move_one = source.move_columns(3, ["StudentID"])
result_move_two = source.move_columns(2, ["Name", "StudentID"])
```

### `move_columns_up`

[`move_columns_up`](../reference/table-operations/select/move-columns-up.md) moves one or more columns to the zeroth column index (the left) in the resulting table.

The following example moves `StudentID` to the leftmost position in the table.

```python test-set=1
result_student_id_first = source.move_columns_up(["StudentID"])
```

### `move_columns_down`

[`move_columns_down`](../reference/table-operations/select/move-columns-down.md) moves one or more columns to the last column index (the right) in the resulting table.

The following example moves `TestGrade` and `HomeworkGrade` to the rightmost position in the table.

```python test-set=1
result_grades_last = source.move_columns_down(["TestGrade", "HomeworkGrade"])
```

## Rename columns in a table

### `rename_columns`

[`rename_columns`](../reference/table-operations/select/rename-columns.md) renames one or more columns in a table. Renaming a column follows the syntax `NewColumnName = OldColumnName`.

> [!IMPORTANT]
> If the new column name conflicts with an existing column name in the table, the existing column is silently replaced.

The following example renames `StudentID` to `ID` and `GPA` to `GradePointAverage`.

```python test-set=1
result_renamed = source.rename_columns(["ID = StudentID", "GradePointAverage = GPA"])
```

### `view` and `select`

Renaming columns with [`view`](../reference/table-operations/select/view.md) and [`select`](../reference/table-operations/select/select.md) follows the same syntax as [`rename_columns`](../reference/table-operations/select/rename-columns.md) (`NewColumnName = OldColumnName`). As we saw previously when [removing columns](#remove-columns-from-a-table), [`select`](../reference/table-operations/select/select.md) and [`view`](../reference/table-operations/select/view.md) take a list of column names as input.

The following example uses both [`select`](../reference/table-operations/select/select.md) and [`view`](../reference/table-operations/select/view.md) to rename the `Name` column to `StudentName`.

> [!IMPORTANT]
> [`select`](../reference/table-operations/select/select.md) creates in-memory columns. Be careful when using [`select`](../reference/table-operations/select/select.md) with large tables, as it can consume a lot of memory.

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
- [Selecting and creating columns](./use-select-view-update.md)
- [`drop_columns`](../reference/table-operations/select/drop-columns.md)
- [`move_columns`](../reference/table-operations/select/move-columns.md)
- [`move_columns_down`](../reference/table-operations/select/move-columns-down.md)
- [`move_columns_up`](../reference/table-operations/select/move-columns-up.md)
- [`rename_columns`](../reference/table-operations/select/rename-columns.md)
- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)
