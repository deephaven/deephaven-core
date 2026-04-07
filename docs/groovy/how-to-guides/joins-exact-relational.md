---
title: Exact and Relational Joins
---

This guide covers exact and relational joins in Deephaven. Exact and relational join operations combine data from two tables based on one or more related key columns.

- An exact join combines tables and only keeps rows where exact matches occur in the key column(s). The following table operations perform an exact join:
  - [`exactJoin`](../reference/table-operations/join/exact-join.md)
  - [`naturalJoin`](../reference/table-operations/join/natural-join.md)
- A relational join primarily combines rows with exact matches across tables, but can also include rows where no exact match exists, depending on the type of join used. The following table operations exemplify different relational joins:
  - [`join`](../reference/table-operations/join/join.md)
  - [`leftOuterJoin`](../reference/table-operations/join/left-outer-join.md)
  - [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md)

Exact and relational joins differ from time-series and range joins. For a detailed guide, see [Joins: time-series and range](./joins-timeseries-range.md).

To join three or more table operations with one operation, see the [`multijoin`](../reference/table-operations/join/multijoin.md) operation documented in a [later section](#join-three-or-more-tables) of this article.

## Syntax

Following convention, the tables being joined together will be referred to as the "left table" and the "right table":

- The left table is the base table to which data is added.
- The right table is the source of data added to the left table.

One or more columns will be used as keys to match data between the left and right tables. This format is fundamental for writing join statements in Deephaven. However, the syntax can vary depending on the circumstances.

The basic syntax for [`join`](../reference/table-operations/join/join.md), [`exactJoin`](../reference/table-operations/join/exact-join.md), and [`naturalJoin`](../reference/table-operations/join/natural-join.md) is as follows:

```groovy syntax
// Include all non-key columns
result = leftTable.joinMethod(rightTable, columnsToMatch)

// Include only some non-key columns (ColumnsToAdd)
result = leftTable.joinMethod(rightTable, columnsToMatch, columnsToAdd)
```

Where `rightTable` is the table to join with, and `columnsToMatch` and `columnsToAdd` are the String names for columns to match and add, respectively.

The basic syntax for [`leftOuterJoin`](../reference/table-operations/join/left-outer-join.md) and [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md) are as follows:

```groovy syntax
// Port all non-key columns
result = outerJoinMethod(leftTable, rightTable, columnsToMatch)

// Port only some non-key columns (ColumnsToAdd)
result = outerJoinMethod(leftTable, rightTable, columnsToMatch, columnsToAdd)
```

> [!NOTE]
> [`leftOuterJoin`](../reference/table-operations/join/left-outer-join.md) and [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md) are currently experimental. The API may change in the future.

Outside of the left and right tables, exact and relational joins take up to two more arguments. The first is required, while the second is optional:

- `columnsToMatch`: The key column(s) on which to look for exact matches. Columns of any data type can be used as key columns, but corresponding match columns in the left and right table _must_ be of the same data type.
- `columnsToAdd` (Optional): The column(s) in the right table to join to the left table. If not specified, all columns are joined.

### Match columns with different names

When two tables can be joined, their match column(s) often don't have identical names. The syntax below joins `leftTable` and `rightTable` on `ColumnToMatchLeft` and `ColumnToMatchRight`:

```groovy syntax
result = leftTable.joinMethod(rightTable, "columnToMatchLeft = columnToMatchRight", "columnsToJoin")
```

### Multiple match columns

Tables can be joined on more than one match column. The syntax below joins tables on two or more match columns:

```groovy syntax
result = leftTable.joinMethod(rightTable, "Column1", "Column2", "Column3Left = Column3Right")
```

### Rename joined columns

Columns being joined from the right table that have the same name as existing columns in the left table will cause a name conflict error. To avoid this, the `joins` argument can be renamed as a column from the right table. The following example renames the right table's `OldColumnName` column to `NewColumnName`:

```groovy syntax
result = leftTable.joinMethod(rightTable, "ColumnToMatchLeft=ColumnToMatchRight", "NewColumnName=OldColumnName")
```

## Exact joins

The output of an exact join operation appends columns to the left table (from the right table) for rows where an exact key match exists in the right table.

Exact matches fail if multiple matching keys are in the right table for any key in the left table.

There are two available operations to perform an exact match join. They differ based on:

- If all rows from the left table are included.
- How zero matches are handled.

### `exactJoin`

`exactJoin` requires the distinct key set of the left table to be identical to the full set of key column values in the right table. If there is no matching key in the right table for any value in a left table key column, the `exactJoin` will fail. Additionally, the operation will fail if multiple matching keys exist in the right table for any key in the left table.

```groovy order=result,left,right
left = newTable(
    stringCol("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith"),
    intCol("DeptID", 31, 33, 33, 34, 34),
    stringCol("Telephone", "(303) 555-0162", "(303) 555-0149", "(303) 555-0184", "(303) 555-0125", ""),
)

right = newTable(
    intCol("DeptID", 31, 33, 34, 35),
    stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
    stringCol("DeptTelephone", "(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171"),
)

result = left.exactJoin(right, "DeptID")
```

### `naturalJoin`

`naturalJoin` allows for cases when there are no matching keys in the right table for particular values in the key column of the left table. If no matching key exists in the right table, appended column values are simply `NULL`. Similarly to `exactJoin`, if there are multiple key matches in the right table, the operation will fail.

```groovy test-set=1 order=result,left,right
left = newTable(
    stringCol("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers", "DelaCruz"),
    intCol("DeptID", 31, 33, 33, 34, 34, 36, NULL_INT),
    stringCol("Telephone", "(303) 555-0162", "(303) 555-0149", "(303) 555-0184", "(303) 555-0125", "", "", "(303) 555-0160"),
)

right = newTable(
    intCol("DeptID", 31, 33, 34, 35),
    stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
    stringCol("DeptTelephone", "(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171"),
)

result = left.naturalJoin(right, "DeptID")
```

## Relational joins

In contrast to exact joins, relational joins provide operations where multiple key matches in the right table will not result in an error.

The three relational join methods differ in how zero exact matches are handled.

### `join`

The output table from a `join` operation contains rows with matching values in both tables. Rows without matching values are not included in the result.

```groovy order=result,left,right
left = newTable(
    stringCol("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers", "DelaCruz"),
    intCol("DeptID", 31, 33, 33, 34, 34, 36, NULL_INT),
    stringCol("Telephone", "(303) 555-0162", "(303) 555-0149", "(303) 555-0184", "(303) 555-0125", "", "", "(303) 555-0160"),
)

right = newTable(
    intCol("DeptID", 31, 33, 34, 35),
    stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
    stringCol("DeptTelephone", "(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171"),
)

result = left.join(right, "DeptID")
```

> [!TIP]
> [`join`](../reference/table-operations/join/join.md) computes the cross product of the left and right tables and subsets the rows based on the arguments. This means it is slow relative to [`naturalJoin`](../reference/table-operations/join/natural-join.md), so [`naturalJoin`](../reference/table-operations/join/natural-join.md) should be preferred in most places.

### `leftOuterJoin`

> [!NOTE]
> This table operation is currently experimental. The API may change in the future.

The output table from a [`leftOuterJoin`](../reference/table-operations/join/left-outer-join.md) operation contains _all_ rows from the left table as well as rows from the right table that have matching keys in the match column(s).

```groovy order=result,left,right
import io.deephaven.engine.util.OuterJoinTools

left = emptyTable(5).update("I = ii", "A = `left`")
right = emptyTable(5).update("I = ii * 2", "B = `right`", "C = Math.sin(I)")

result = OuterJoinTools.leftOuterJoin(left, right, "I")
```

### `fullOuterJoin`

> [!NOTE]
> This table operation is currently experimental. The API may change in the future.

The output table from a [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md) operation contains all rows in the key identifier columns from both tables. Keys that exist in one table but not the other project null values into the respective non-key columns for the unmatched row.

```groovy order=result,left,right
import io.deephaven.engine.util.OuterJoinTools

left = emptyTable(5).update("I = ii", "A = `left`")
right = emptyTable(5).update("I = ii * 2", "B = `right`", "C = Math.sin(I)")

result = OuterJoinTools.fullOuterJoin(left, right, "I")
```

## Join three or more tables

The [`multijoin`](../reference/table-operations/join/multijoin.md) operation joins three or more tables. It was developed to improve the join speed by taking advantage of the potential to share a single hash table and exploit concurrency.

[`MultiJoin.of`](../reference/table-operations/join/multijoin.md) joins three or more tables together in the same way that [`naturalJoin`](../reference/table-operations/join/natural-join.md) joins two tables together. The result of [`MultiJoin.of`](../reference/table-operations/join/multijoin.md) is not a typical table, but rather a `MultiJoinTable` object, so calling the `table` method is necessary for most use cases.

There are two ways to use [`MultiJoin.of`](../reference/table-operations/join/multijoin.md): with constituent tables or with one or more `MultiJoinInput` objects.

### With constituent tables

Using constituent tables is syntactically simple. The syntax is as follows:

```groovy syntax
MultiJoinTable mjTable = MultiJoinFactory.of(keys, tables...)
```

- `keys` is a single string of comma-separated key column names; for example, `"key1, key2"`.
- `tables` is any number of tables to merge; for example, `table1, table2, table3`.

Using constituent tables requires that all tables have identical key column names and that _all_ of the tables' output rows are desired.

The following example joins three tables that correspond to letter grades for students at three different grade levels.

```groovy order=result,Grade5,Grade6,Grade7
// import multijoin classes
import io.deephaven.engine.table.MultiJoinFactory
import io.deephaven.engine.table.MultiJoinTable

// create tables
Grade5 = newTable(
    stringCol("Name","Mark", "Austin", "Jane", "Alex", "May"),
    stringCol("Grade", "A", "A", "C", "B", "A"),
)

Grade6 = newTable(
    stringCol("Name","Sandra", "Andy", "Kathy", "June", "October"),
    stringCol("Grade", "B", "C", "D", "A", "A"),
)

Grade7 = newTable(
    stringCol("Name","Lando", "Han", "Luke", "Ben", "Caleb"),
    stringCol("Grade", "C", "B", "A", "C", "B"),
)

// create a MultiJoinTable object and join the three tables
MultiJoinTable mtTable = MultiJoinFactory.of("Name, Grade", Grade5, Grade6, Grade7)

// access the multijoin object's internal table
result = mtTable.table()
```

### With `MultiJoinInput` objects

Using `MultiJoinInput` objects as inputs for [`MultiJoin.of`](../reference/table-operations/join/multijoin.md) is syntactically more complex than using [constituent tables](#with-constituent-tables), but allows for more flexibility. The syntax for creating a `MultiJoinInput` object is as follows:

```groovy syntax
// create a MultiJoinInput array
MultiJoinInput mjInputArr = new MultiJoinInput[] {
    MultiJoinInput.of(t1, "Key1=A,Key2=B", "C1=C,D1=D"),
    MultiJoinInput.of(y2, "Key1=A,Key2=B", "C2=C,D2=D")
}

// create a MultiJoinTable object
MultiJoinTable mjTable = MultiJoinFactory.of(mjInputArr);
```

The following example demonstrates the creation of a `MultiJoinTable` using a `MultiJoinInput` array.

- First, we create three tables.
- Then, we create a `MultiJoinInput` array, which is used as an input for the `MultiJoinFactory.of` method instead of a String array of keys and a list of tables.
- Finally, we retrieve the underlying table.

> [!IMPORTANT]
> Note that if we call `MultiJoinInput.of` with only a table and a key, _all_ of the table's constituent columns will be included in the join.

```groovy order=result,t1,t2,t3
// import multijoin classes
import io.deephaven.engine.table.MultiJoinFactory
import io.deephaven.engine.table.MultiJoinTable
import io.deephaven.engine.table.MultiJoinInput

// create tables
t1 = newTable(intCol("C1", 1, 2), intCol("C2", 1, 1), intCol("S1", 10, 11))

t2 = newTable(intCol("C1", 3123, 62364), intCol("C3", 56, 99), intCol("S2", 10, 11))

t3 = newTable(intCol("C1", 44, 3), intCol("C4", 182374, 1231), intCol("S3", 44, 2313))

// create a MultiJoinInput array
mjArr = new MultiJoinInput[] {MultiJoinInput.of(t1, "Key=C1"), MultiJoinInput.of(t2, "Key=C1"), MultiJoinInput.of(t3, "Key=C1")}

// create a MultiJoinTable object
mjTable = MultiJoinFactory.of(mjArr)

// retrieve the underlying table
result = mjTable.table()
```

In this example, we repeat the previous example but specify which columns we want to include in the join.

```groovy order=result
// import multijoin classes
import io.deephaven.engine.table.MultiJoinFactory
import io.deephaven.engine.table.MultiJoinTable
import io.deephaven.engine.table.MultiJoinInput

// create tables
t1 = newTable(intCol("C1", 1, 2), intCol("C2", 1, 1), intCol("S1", 10, 11))

t2 = newTable(intCol("C1", 3123, 62364), intCol("C3", 56, 99), intCol("S2", 10, 11))

t3 = newTable(intCol("C1", 44, 3), intCol("C4", 182374, 1231), intCol("S3", 44, 2313))

// create a MultiJoinInput array
mjArr = new MultiJoinInput[] {MultiJoinInput.of(t1, "Key=C1", "C2"), MultiJoinInput.of(t2, "Key=C1", "S2"), MultiJoinInput.of(t3, "Key=C1", "C4")}

// create multijoin object and retrieve the underlying table
result = MultiJoinFactory.of(mjArr).table()
```

Now only the columns specified appear in the output table.

## Which method should you use?

Choosing the right join method can be tricky, so here are some things to consider when choosing between what's available:

- How should multiple exact matches be handled?
  - [`exactJoin`](../reference/table-operations/join/exact-join.md) and [`naturalJoin`](../reference/table-operations/join/natural-join.md) raise an error.
  - [`join`](../reference/table-operations/join/join.md), [`leftOuterJoin`](../reference/table-operations/join/left-outer-join.md), and [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md) do not raise an error.
- How should zero exact matches be handled?
  - [`exactJoin`](../reference/table-operations/join/exact-join.md) raises an error.
  - [`naturalJoin`](../reference/table-operations/join/natural-join.md) joins a null value.
- What data should be included in the result?
  - [`join`](../reference/table-operations/join/join.md) includes only rows that have matching values in _both_ tables.
  - [`leftOuterJoin`](../reference/table-operations/join/left-outer-join.md) includes all rows from the left table, and only rows from the right table that match those in the left table.
  - [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md) includes all rows from _both_ tables.

For help in choosing a method that uses inexact matches to join tables, see [here](./joins-timeseries-range.md).

The following figure presents a flowchart to help choose the right join method for your query.

<Svg src='../assets/conceptual/joins3.svg' style={{height: 'auto', maxWidth: '100%'}} />

## Related documentation

- [Time series and range joins](./joins-timeseries-range.md)
- [`exactJoin`](../reference/table-operations/join/exact-join.md)
- [`fullOuterJoin`](../reference/table-operations/join/full-outer-join.md)
- [`join`](../reference/table-operations/join/join.md)
- [`leftOuterJoin`](../reference/table-operations/join/left-outer-join.md)
- [`MultiJoin.of`](../reference/table-operations/join/multijoin.md)
- [`naturalJoin`](../reference/table-operations/join/natural-join.md)
