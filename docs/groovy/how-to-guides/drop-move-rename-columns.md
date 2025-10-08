---
title: Drop, move, and rename columns
---

Queries often call for dropping, moving, or renaming columns in a table. This guide covers doing so programmatically.

The following methods drop columns from a table:

- [`dropColumns`](../reference/table-operations/select/drop-columns.md)
- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)

The following methods move columns in a table:

- [`moveColumns`](../reference/table-operations/select/move-columns.md)
- [`moveColumnsDown`](../reference/table-operations/select/move-columns-down.md)
- [`moveColumnsUp`](../reference/table-operations/select/move-columns-up.md)
- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)

The following methods rename columns in a table:

- [`renameColumns`](../reference/table-operations/select/rename-columns.md)
- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)

The examples in this guide will use the following table:

```groovy test-set=1
source = newTable(
        stringCol("Name", "Andy", "Claire", "Jane", "Steven"),
        intCol("StudentID", 1, 2, 3, 4),
        intCol("TestGrade", 85, 95, 88, 72),
        intCol("HomeworkGrade", 85, 95, 90, 95),
        doubleCol("GPA", 3.0, 4.0, 3.7, 2.8),
)
```

## Remove columns from a table

### `dropColumns`

[`dropColumns`](../reference/table-operations/select/drop-columns.md) removes one or more columns from a table based on the column names it's given.

```groovy test-set=1 order=resultDropOneCol,resultDropTwoCols
resultDropOneCol = source.dropColumns("GPA")
resultDropTwoCols = source.dropColumns("TestGrade", "HomeworkGrade")
```

### `view`

[`view`](../reference/table-operations/select/view.md) removes any columns not given as input that are present in the source table. If [`view`](../reference/table-operations/select/view.md) is not given any input, it returns a table the same as the source table but with all formula columns.

> [!IMPORTANT]
> Formula columns initially store only the formulas used to create columnar data. Values are calculated on the fly as needed.

The following code block uses [`view`](../reference/table-operations/select/view.md) to remove the `StudentID` column from the `source` table.

```groovy test-set=1
resultNoStudentId = source.view("Name", "TestGrade", "HomeworkGrade", "GPA")
```

### `select`

[`select`](../reference/table-operations/select/select.md) is similar to [`view`](../reference/table-operations/select/view.md) in that it removes any columns not given as input that are present in the source table. However, unlike [`view`](../reference/table-operations/select/view.md),[`select`](../reference/table-operations/select/select.md) creates in-memory columns.

> [!IMPORTANT]
> In-memory columns store _all_ of their data in memory. This includes columns of any type, such as formula or memoized columns. Be careful when using [`select`](../reference/table-operations/select/select.md) with large tables, as it can consume a lot of memory.

The following code block uses [`select`](../reference/table-operations/select/select.md) to remove the `Name` column from the `source` table.

```groovy test-set=1
resultNoName = source.select("StudentID", "TestGrade", "HomeworkGrade", "GPA")
```

## Move columns in a table

### `moveColumns`

[`moveColumns`](../reference/table-operations/select/move-columns.md) moves one or more columns to a specified column index in the resulting table.

The following code block uses [`moveColumns`](../reference/table-operations/select/move-columns.md) twice: once to move a single column further right, and once to move multiple columns further right.

```groovy test-set=1 order=resultMoveOne,resultMoveTwo
resultMoveOne = source.moveColumns(3, "StudentID")
resultMoveTwo = source.moveColumns(2, "Name", "StudentID")
```

### `moveColumnsUp`

[`moveColumnsUp`](../reference/table-operations/select/move-columns-up.md) moves one or more columns to the zeroth column index (the left) in the resulting table.

The following example moves `StudentID` to the leftmost position in the table.

```groovy test-set=1
resultStudentIdFirst = source.moveColumnsUp("StudentID")
```

### `moveColumnsDown`

[`moveColumnsDown`](../reference/table-operations/select/move-columns-down.md) moves one or more columns to the last column index (the right) in the resulting table.

The following example moves `TestGrade` and `HomeworkGrade` to the rightmost position in the table.

```groovy test-set=1
resultGradesLast = source.moveColumnsDown("TestGrade", "HomeworkGrade")
```

## Rename columns in a table

### `renameColumns`

[`renameColumns`](../reference/table-operations/select/rename-columns.md) renames one or more columns in a table. Renaming a column follows the syntax `NewColumnName = OldColumnName`.

> [!IMPORTANT]
> If the new column name conflicts with an existing column name in the table, the existing column will be silently replaced.

The following example renames `StudentID` to `ID` and `GPA` to `GradePointAverage`.

```groovy test-set=1
resultRenamed = source.renameColumns("ID = StudentID", "GradePointAverage = GPA")
```

### `view` and `select`

Renaming columns with [`view`](../reference/table-operations/select/view.md) and [`select`](../reference/table-operations/select/select.md) follows the same syntax as [`renameColumns`](../reference/table-operations/select/rename-columns.md) (`NewColumnName = OldColumnName`). As we saw previously when [removing columns](#remove-columns-from-a-table), [`select`](../reference/table-operations/select/select.md) and [`view`](../reference/table-operations/select/view.md) take a list of column names as input.

The following example uses both [`select`](../reference/table-operations/select/select.md) and [`view`](../reference/table-operations/select/view.md) to rename the `Name` column to `StudentName`.

> [!IMPORTANT]
> [`select`](../reference/table-operations/select/select.md) creates in-memory columns. Be careful when using [`select`](../reference/table-operations/select/select.md) with large tables, as it can consume a lot of memory.

```groovy test-set=1 order=resultSelectRenamed,resultViewRenamed
resultSelectRenamed = source.select(
    "StudentName = Name", "StudentID", "TestGrade", "HomeworkGrade", "GPA"
)
resultViewRenamed = source.view(
    "StudentName = Name", "StudentID", "TestGrade", "HomeworkGrade", "GPA"
)
```

## Related documentation

- [Create a new table](./new-and-empty-table.md#newtable)
- [Selecting and creating columns](./use-select-view-update.md)
- [`dropColumns`](../reference/table-operations/select/drop-columns.md)
- [`moveColumns`](../reference/table-operations/select/move-columns.md)
- [`moveColumns_down`](../reference/table-operations/select/move-columns-down.md)
- [`moveColumns_up`](../reference/table-operations/select/move-columns-up.md)
- [`renameColumns`](../reference/table-operations/select/rename-columns.md)
- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)
