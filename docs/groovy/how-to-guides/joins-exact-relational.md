---
title: Exact and relational joins
---

This guide covers exact and relational joins in Deephaven. Both kinds of join combine data from two tables by matching values in one or more key columns. This guide calls the table that receives data the _left table_ and the table that supplies it the _right table_.

- An exact join adds columns from the right table to every row of the left table, using at most one matching right row per key. The result has the same rows as the left table. These table operations perform an exact join:
  - [`exactJoin`](../reference/table-operations/join/exact-join.md)
  - [`naturalJoin`](../reference/table-operations/join/natural-join.md)
- A relational join pairs each left row with every matching right row, so one key can produce several result rows. Depending on the operation, the result can also include rows that have no match. These table operations perform a relational join:
  - [`join`](../reference/table-operations/join/join.md)
  - [`leftOuterJoin`](../reference/table-operations/join/left-outer-join.md)
  - [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md)

To join three or more tables on matching key values in one operation, use [`MultiJoinFactory.of`](../reference/table-operations/join/multijoin.md), described in [Join three or more tables](#join-three-or-more-tables). The key columns can have different names in each table.

Exact and relational joins match key values exactly. To match on the nearest value or on a range of values, see [Inexact, time-series, and range joins](./joins-timeseries-range.md).

## Which method should you use?

Answer these questions to choose a join method:

- What should happen when the right table has more than one match for a key?
  - [`exactJoin`](../reference/table-operations/join/exact-join.md) fails.
  - [`naturalJoin`](../reference/table-operations/join/natural-join.md) fails by default. It can instead [keep the first or last matching row](#naturaljoin).
  - [`join`](../reference/table-operations/join/join.md), [`leftOuterJoin`](../reference/table-operations/join/left-outer-join.md), and [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md) include a result row for every match.
- What should happen to a left-table row with no match?
  - [`exactJoin`](../reference/table-operations/join/exact-join.md) fails.
  - [`naturalJoin`](../reference/table-operations/join/natural-join.md), [`leftOuterJoin`](../reference/table-operations/join/left-outer-join.md), and [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md) keep the row and fill the right table's columns with null values.
  - [`join`](../reference/table-operations/join/join.md) leaves the row out of the result.
- Should the result include right-table rows that match nothing in the left table?
  - Only [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md) includes them, with null values in the left table's columns.
- Are you joining three or more tables on the same keys, even if the key column names differ, with at most one row per key in each table?
  - Use [`MultiJoinFactory.of`](../reference/table-operations/join/multijoin.md).

The following flowchart walks through the same choices for two tables, including the inexact joins described in [Inexact, time-series, and range joins](./joins-timeseries-range.md).

<Svg src='../assets/conceptual/joins3.svg' style={{height: 'auto', maxWidth: '100%'}} />

## Syntax

[`join`](../reference/table-operations/join/join.md), [`exactJoin`](../reference/table-operations/join/exact-join.md), and [`naturalJoin`](../reference/table-operations/join/natural-join.md) are methods of the left table:

```groovy syntax
// Add all non-key columns from the right table
result = leftTable.joinMethod(rightTable, columnsToMatch)

// Add only some non-key columns from the right table
result = leftTable.joinMethod(rightTable, columnsToMatch, columnsToAdd)
```

[`leftOuterJoin`](../reference/table-operations/join/left-outer-join.md) and [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md) are static methods of the [`OuterJoinTools`](/core/javadoc/io/deephaven/engine/util/OuterJoinTools.html) class that take both tables as arguments:

```groovy syntax
import io.deephaven.engine.util.OuterJoinTools

// Add all non-key columns from the right table
result = OuterJoinTools.outerJoinMethod(leftTable, rightTable, columnsToMatch)

// Add only some non-key columns from the right table
result = OuterJoinTools.outerJoinMethod(leftTable, rightTable, columnsToMatch, columnsToAdd)
```

Besides the two tables, these operations take two main arguments. Each is a `String` of comma-separated column names or expressions:

- `columnsToMatch`: The key columns to match. Required for `exactJoin`, `naturalJoin`, and the outer joins. Optional for `join`, which pairs every left row with every right row when `columnsToMatch` is omitted.
- `columnsToAdd` (optional): The columns from the right table to add to the left table. If omitted, the join adds all non-key columns from the right table.

A key column can be of any data type, but each pair of matched columns in the left and right tables _must_ have the same data type.

### Match columns with different names

The key columns in two tables often have different names. The syntax below joins `leftTable` and `rightTable` on `ColumnToMatchLeft` and `ColumnToMatchRight`:

```groovy syntax
result = leftTable.joinMethod(rightTable, "ColumnToMatchLeft = ColumnToMatchRight", "ColumnsToAdd")
```

### Multiple match columns

To join tables on more than one key column, list them all in a single comma-separated `String`:

```groovy syntax
result = leftTable.joinMethod(rightTable, "Column1, Column2, Column3Left = Column3Right")
```

### Rename joined columns

If a column added from the right table has the same name as a column in the left table, the join fails with a name conflict. To avoid this, rename the column in the `columnsToAdd` argument. The following example adds the right table's `OldColumnName` column to the result as `NewColumnName`:

```groovy syntax
result = leftTable.joinMethod(rightTable, "ColumnToMatchLeft = ColumnToMatchRight", "NewColumnName = OldColumnName")
```

## Example tables

The examples in the [Exact joins](#exact-joins) and [Relational joins](#relational-joins) sections use two tables. `employees` lists employees and the ID of the department each one works in. Rogers works in department 36, which doesn't exist, and DelaCruz has no department. `departments` lists departments by ID. No employee works in Marketing (department 35).

```groovy test-set=1 order=employees,departments
employees = newTable(
    stringCol("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers", "DelaCruz"),
    intCol("DeptID", 31, 33, 33, 34, 34, 36, NULL_INT),
    stringCol("Telephone", "(303) 555-0162", "(303) 555-0149", "(303) 555-0184", "(303) 555-0125", "", "", "(303) 555-0160"),
)

departments = newTable(
    intCol("DeptID", 31, 33, 34, 35),
    stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
    stringCol("DeptTelephone", "(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171"),
)
```

## Exact joins

An exact join keeps every row of the left table and appends columns from the matching row of the right table. [`exactJoin`](../reference/table-operations/join/exact-join.md) and [`naturalJoin`](../reference/table-operations/join/natural-join.md) differ in how they handle a left-table row with no match, and in whether they can keep one of several matching right rows.

### `exactJoin`

[`exactJoin`](../reference/table-operations/join/exact-join.md) requires every row in the left table to have exactly one matching row in the right table. The operation fails if a left-table key has no match or more than one match in the right table. The operation ignores right-table keys that have no match in the left table.

Calling `exactJoin` on the whole [`employees`](#example-tables) table fails because Rogers and DelaCruz have no matching department. This example first keeps only the employees whose department exists, and then adds the department columns:

```groovy test-set=1 order=result,assigned
assigned = employees.where("DeptID in 31, 33, 34")

result = assigned.exactJoin(departments, "DeptID")
```

### `naturalJoin`

[`naturalJoin`](../reference/table-operations/join/natural-join.md) allows left-table rows that have no match in the right table. The appended columns in those rows are null. In this example, Rogers and DelaCruz get null department columns:

```groovy test-set=1 order=result
result = employees.naturalJoin(departments, "DeptID")
```

By default, `naturalJoin` fails if the right table has more than one row for a key. To keep one of the matching rows instead, pass [`NaturalJoinType`](/core/javadoc/io/deephaven/api/NaturalJoinType.html) `.FIRST_MATCH` or `NaturalJoinType.LAST_MATCH` as the last argument. The following example swaps the tables, using `departments` as the left table, and adds the first matching employee to each department. Departments 33 and 34 each have two employees, so the default join type would fail. Marketing has no employees, so its employee columns are null:

```groovy test-set=1 order=result
import io.deephaven.api.NaturalJoinType

result = departments.naturalJoin(employees, "DeptID", NaturalJoinType.FIRST_MATCH)
```

## Relational joins

Unlike exact joins, which use at most one matching right row per key, relational joins keep every matching right row. Each left row appears in the result once for every matching right row. The three relational joins differ in which unmatched rows they keep. The examples in this section use `departments` as the left table and `employees` as the right table, so a department with several employees produces several result rows.

### `join`

The output table from a [`join`](../reference/table-operations/join/join.md) contains a row for every pair of matching left and right rows. The result leaves out rows that have no match in the other table. In this example, the Engineering and Clerical departments each appear twice, once per employee. Marketing doesn't appear because no employee works there:

```groovy test-set=1 order=result
result = departments.join(employees, "DeptID")
```

> [!TIP]
> Because `join` includes every matching combination of left and right rows, its output can be much larger than either input. A large result also costs more to maintain on [ticking tables](../conceptual/table-update-model.md), whose rows change over time. If each left row needs at most one right match, use [`naturalJoin`](../reference/table-operations/join/natural-join.md) instead. It is faster, and its result has the same number of rows as the left table.

### `leftOuterJoin`

> [!NOTE]
> This table operation is currently experimental. The API may change in the future.

The output table from a [`leftOuterJoin`](../reference/table-operations/join/left-outer-join.md) contains every row that `join` would produce, plus each left-table row that has no match, with null values in the right table's columns. In this example, Marketing appears with null employee columns:

```groovy test-set=1 order=result
import io.deephaven.engine.util.OuterJoinTools

result = OuterJoinTools.leftOuterJoin(departments, employees, "DeptID")
```

### `fullOuterJoin`

> [!NOTE]
> This table operation is currently experimental. The API may change in the future.

The output table from a [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md) contains every row that `leftOuterJoin` would produce, plus each right-table row that has no match, with null values in the left table's columns. In this example, Marketing appears with null employee columns, and Rogers and DelaCruz appear with null department names:

```groovy test-set=1 order=result
import io.deephaven.engine.util.OuterJoinTools

result = OuterJoinTools.fullOuterJoin(departments, employees, "DeptID")
```

## Join three or more tables

[`MultiJoinFactory.of`](../reference/table-operations/join/multijoin.md) joins any number of tables on a common set of key columns in a single operation. The result has one row for each distinct key found in any input table. Each input table adds its columns to that row. As with [`naturalJoin`](../reference/table-operations/join/natural-join.md) in its default mode, an input table can have at most one row per key, and `MultiJoinFactory.of` fails if an input has duplicate keys. An input table with no row for a key contributes null values.

`MultiJoinFactory.of` returns a [`MultiJoinTable`](../reference/table-operations/join/MultiJoinTable.md) object rather than a table. To get the result table, call its `table` method.

There are two ways to call `MultiJoinFactory.of`:

- Pass the tables directly. Every table must use the same key column names, and the result includes every non-key column from every table.
- Pass [`MultiJoinInput`](../reference/table-operations/join/MultiJoinInput.md) objects. Each `MultiJoinInput` specifies one table, the mapping from its key columns to the result's key columns, and the columns to add from it. The columns to add are optional.

### Pass the tables directly

To join the tables directly, pass a `String` of comma-separated key column names, such as `"Key1, Key2"`, followed by the tables:

```groovy syntax
MultiJoinTable mjTable = MultiJoinFactory.of(columnsToMatch, tables...)
```

The following example joins three tables of letter grades for students in grades 5, 6, and 7. Not every student appears in every table, so some grades in the result are null:

```groovy test-set=2 order=result,grade5,grade6,grade7
import io.deephaven.engine.table.MultiJoinFactory
import io.deephaven.engine.table.MultiJoinTable

grade5 = newTable(
    stringCol("Name", "Mark", "Austin", "Sandra", "Andy", "Caleb"),
    stringCol("Grade5", "A", "A", "C", "B", "A"),
)

grade6 = newTable(
    stringCol("Name", "Sandra", "Andy", "Kathy", "June", "Caleb"),
    stringCol("Grade6", "B", "C", "D", "A", "A"),
)

grade7 = newTable(
    stringCol("Name", "Austin", "Kathy", "Sandra", "Mark", "Caleb"),
    stringCol("Grade7", "C", "B", "A", "C", "B"),
)

MultiJoinTable multiJoinTable = MultiJoinFactory.of("Name", grade5, grade6, grade7)

result = multiJoinTable.table()
```

### Pass `MultiJoinInput` objects

Use `MultiJoinInput` objects when the key columns have different names in different tables, or when you want only some of a table's columns in the result. Create one `MultiJoinInput` per table with `MultiJoinInput.of`, and then pass them all to `MultiJoinFactory.of`:

```groovy syntax
import io.deephaven.engine.table.MultiJoinFactory
import io.deephaven.engine.table.MultiJoinInput
import io.deephaven.engine.table.MultiJoinTable

// Add all non-key columns from t1
input1 = MultiJoinInput.of(t1, "KeyColumn")
// Match t2's OtherKey column to KeyColumn, and add only Column1 and Column2
input2 = MultiJoinInput.of(t2, "KeyColumn = OtherKey", "Column1, Column2")

MultiJoinTable mjTable = MultiJoinFactory.of(input1, input2)
```

The following example adds each student's club to the grades from the [previous example](#pass-the-tables-directly). The `clubs` table names its key column `Student` instead of `Name`, and the result includes only its `Club` column:

```groovy test-set=2 order=result,clubs
import io.deephaven.engine.table.MultiJoinFactory
import io.deephaven.engine.table.MultiJoinInput

clubs = newTable(
    stringCol("Student", "Andy", "Kathy", "Mark", "June"),
    stringCol("Club", "Chess", "Drama", "Robotics", "Chess"),
    stringCol("Advisor", "Lee", "Ortiz", "Patel", "Lee"),
)

result = MultiJoinFactory.of(
    MultiJoinInput.of(grade5, "Name"),
    MultiJoinInput.of(grade6, "Name"),
    MultiJoinInput.of(grade7, "Name"),
    MultiJoinInput.of(clubs, "Name = Student", "Club"),
).table()
```

## Related documentation

- [Inexact, time-series, and range joins](./joins-timeseries-range.md)
- [`exactJoin`](../reference/table-operations/join/exact-join.md)
- [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md)
- [`join`](../reference/table-operations/join/join.md)
- [`leftOuterJoin`](../reference/table-operations/join/left-outer-join.md)
- [`MultiJoinFactory.of`](../reference/table-operations/join/multijoin.md)
- [`MultiJoinInput`](../reference/table-operations/join/MultiJoinInput.md)
- [`MultiJoinTable`](../reference/table-operations/join/MultiJoinTable.md)
- [`naturalJoin`](../reference/table-operations/join/natural-join.md)
