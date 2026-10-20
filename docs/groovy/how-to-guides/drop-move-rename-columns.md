---
title: Drop, move, and rename columns
---

Queries often call for dropping, moving, or renaming columns in a table. This guide covers doing so programmatically.

Like the rest of Deephaven's table operations, these work the same on static and real-time tables. [`dropColumns`](../reference/table-operations/select/drop-columns.md), [`renameColumns`](../reference/table-operations/select/rename-columns.md), and the move methods don't copy any data: the result shares the source table's columns and updates whenever the source does.

The following table lists the methods that do each task:

| Task           | Methods                                                                                                                                                                                                                                                                                                                                                                                                                          |
| -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Drop columns   | [`dropColumns`](../reference/table-operations/select/drop-columns.md), [`select`](../reference/table-operations/select/select.md), [`view`](../reference/table-operations/select/view.md)                                                                                                                                                                                                                                        |
| Move columns   | [`moveColumns`](../reference/table-operations/select/move-columns.md), [`moveColumnsUp`](../reference/table-operations/select/move-columns-up.md), [`moveColumnsDown`](../reference/table-operations/select/move-columns-down.md), [`select`](../reference/table-operations/select/select.md), [`view`](../reference/table-operations/select/view.md)                                                                            |
| Rename columns | [`renameColumns`](../reference/table-operations/select/rename-columns.md), [`moveColumns`](../reference/table-operations/select/move-columns.md), [`moveColumnsUp`](../reference/table-operations/select/move-columns-up.md), [`moveColumnsDown`](../reference/table-operations/select/move-columns-down.md), [`select`](../reference/table-operations/select/select.md), [`view`](../reference/table-operations/select/view.md) |

The examples in this guide use the following table:

```groovy test-set=1
source = newTable(
    stringCol("Name", "Andy", "Claire", "Jane", "Steven"),
    intCol("StudentID", 1, 2, 3, 4),
    intCol("TestGrade", 85, 95, 88, 72),
    intCol("HomeworkGrade", 85, 95, 90, 95),
    doubleCol("GPA", 3.0, 4.0, 3.7, 2.8),
)
```

## Drop columns from a table

[`dropColumns`](../reference/table-operations/select/drop-columns.md) takes the columns to remove. [`view`](../reference/table-operations/select/view.md) and [`select`](../reference/table-operations/select/select.md) take the columns to keep, so they suit a table where you keep only a few. The two differ in when they compute and store data, as the sections below show. For a side-by-side comparison, see [Choose the right column selection method](./use-select-view-update.md#choose-the-right-column-selection-method).

### `dropColumns`

[`dropColumns`](../reference/table-operations/select/drop-columns.md) removes the columns you name from a table.

```groovy test-set=1 order=resultDropOneCol,resultDropTwoCols
resultDropOneCol = source.dropColumns("GPA")
resultDropTwoCols = source.dropColumns("TestGrade", "HomeworkGrade")
```

### `view`

[`view`](../reference/table-operations/select/view.md) keeps only the columns you list and removes the rest of the source table's columns. Columns given by name reuse the source table's data, while columns defined by a [formula](./formulas.md) (such as `Total = TestGrade + HomeworkGrade`) become formula columns.

> [!NOTE]
> A formula column stores only its formula. Deephaven calculates each value when it's accessed.

The following code block uses [`view`](../reference/table-operations/select/view.md) to remove the `StudentID` column from the `source` table.

```groovy test-set=1
resultNoStudentId = source.view("Name", "TestGrade", "HomeworkGrade", "GPA")
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

```groovy test-set=1
resultNoName = source.select("StudentID", "TestGrade", "HomeworkGrade", "GPA")
```

## Move columns in a table

Choose a method based on where the columns should go:

- To place columns at a given index, use [`moveColumns`](../reference/table-operations/select/move-columns.md).
- To send columns to the left edge, use [`moveColumnsUp`](../reference/table-operations/select/move-columns-up.md).
- To send columns to the right edge, use [`moveColumnsDown`](../reference/table-operations/select/move-columns-down.md).
- To list every column in a new order, use [`view`](../reference/table-operations/select/view.md) or [`select`](../reference/table-operations/select/select.md).
- To move and rename a column in one call, pass a rename expression such as `"ID = StudentID"` to any of the move methods. See [Move and rename columns together](#move-and-rename-columns-together).

### `moveColumns`

[`moveColumns`](../reference/table-operations/select/move-columns.md) moves one or more columns to a specified column index in the resulting table. Column indexes start at 0, the leftmost column.

The following code block uses [`moveColumns`](../reference/table-operations/select/move-columns.md) twice: once to move a single column further right, and once to move multiple columns further right.

```groovy test-set=1 order=resultMoveOne,resultMoveTwo
resultMoveOne = source.moveColumns(3, "StudentID")
resultMoveTwo = source.moveColumns(2, "Name", "StudentID")
```

In `resultMoveOne`, `StudentID` lands at index 3, just before `GPA`. In `resultMoveTwo`, `Name` and `StudentID` land at indexes 2 and 3, in the order you list them.

### `moveColumnsUp`

[`moveColumnsUp`](../reference/table-operations/select/move-columns-up.md) moves one or more columns to the left edge of the resulting table, starting at index 0.

The following example moves `StudentID` to the leftmost position in the table.

```groovy test-set=1
resultStudentIdFirst = source.moveColumnsUp("StudentID")
```

### `moveColumnsDown`

[`moveColumnsDown`](../reference/table-operations/select/move-columns-down.md) moves one or more columns to the right edge of the resulting table.

The following example moves `TestGrade` and `HomeworkGrade` to the right edge of the table, so they become the last two columns.

```groovy test-set=1
resultGradesLast = source.moveColumnsDown("TestGrade", "HomeworkGrade")
```

### Reorder with `view` and `select`

[`view`](../reference/table-operations/select/view.md) and [`select`](../reference/table-operations/select/select.md) output columns in the order you list them. To reorder a table, list every column in the order you want. The following example puts `GPA` first:

```groovy test-set=1
resultGpaFirst = source.view("GPA", "Name", "StudentID", "TestGrade", "HomeworkGrade")
```

### Move and rename columns together

[`moveColumns`](../reference/table-operations/select/move-columns.md), [`moveColumnsUp`](../reference/table-operations/select/move-columns-up.md), and [`moveColumnsDown`](../reference/table-operations/select/move-columns-down.md) also accept rename expressions, such as `"ID = StudentID"`, so you can move and rename a column in one call. A rename expression here follows the same rules as [`renameColumns`](../reference/table-operations/select/rename-columns.md), including the [silent replacement of an existing column](#renamecolumns) with the same name. The following example uses [`moveColumnsUp`](../reference/table-operations/select/move-columns-up.md) to move `StudentID` to the leftmost position and rename it to `ID`:

```groovy test-set=1
resultIdFirst = source.moveColumnsUp("ID = StudentID")
```

## Rename columns in a table

[`renameColumns`](../reference/table-operations/select/rename-columns.md) renames columns in place and keeps the rest of the table's columns in their original order. [`view`](../reference/table-operations/select/view.md) and [`select`](../reference/table-operations/select/select.md) can also rename, but they keep only the columns you list. The move methods can rename a column as they move it; see [Move and rename columns together](#move-and-rename-columns-together).

### `renameColumns`

[`renameColumns`](../reference/table-operations/select/rename-columns.md) renames one or more columns in a table. Renaming a column follows the syntax `NewColumnName = OldColumnName`.

> [!IMPORTANT]
> If the new column name matches an existing column that the same call doesn't also rename, [`renameColumns`](../reference/table-operations/select/rename-columns.md) silently replaces the existing column.

The following example renames `StudentID` to `ID` and `GPA` to `GradePointAverage`.

```groovy test-set=1
resultRenamed = source.renameColumns("ID = StudentID", "GradePointAverage = GPA")
```

### Rename with `view` and `select`

[`view`](../reference/table-operations/select/view.md) and [`select`](../reference/table-operations/select/select.md) rename with the same `NewColumnName = OldColumnName` syntax as [`renameColumns`](../reference/table-operations/select/rename-columns.md). As we saw when [dropping columns](#drop-columns-from-a-table), they keep only the columns you list, so list every column you want to keep along with the rename.

The following example uses both [`select`](../reference/table-operations/select/select.md) and [`view`](../reference/table-operations/select/view.md) to rename the `Name` column to `StudentName`.

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
- [Select and create columns](./use-select-view-update.md)
- [Formulas](./formulas.md)
- [Arrays vs vectors](../conceptual/arrays-vs-vectors.md)
- [`dropColumns`](../reference/table-operations/select/drop-columns.md)
- [`moveColumns`](../reference/table-operations/select/move-columns.md)
- [`moveColumnsDown`](../reference/table-operations/select/move-columns-down.md)
- [`moveColumnsUp`](../reference/table-operations/select/move-columns-up.md)
- [`renameColumns`](../reference/table-operations/select/rename-columns.md)
- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)
