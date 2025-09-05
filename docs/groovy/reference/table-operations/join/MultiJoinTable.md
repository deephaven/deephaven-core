---
title: MultiJoinTable
---

A `MultiJoinTable` object is a container for the table that results from a [Multi-Join](./multijoin.md) operation.

## Methods

```
MultiJoinTable.keyColumns()
MultiJoinTable.table()
```

## Parameters

Neither of `MultiJoinTable`'s methods take parameters.

## Returns

- `MultiJoinTable.keyColumns()` returns the `MultiJoinTable`'s key column names as a collection of strings.
- `MultiJoinTable.table()` returns the `MultiJoinTable`'s underlying table.

## Examples

```groovy order=t1,t2,t3,result
// import multijoin classes
import io.deephaven.engine.table.MultiJoinFactory
import io.deephaven.engine.table.MultiJoinTable

// create tables
t1 = newTable(stringCol("Letter", "A", "B", "C", "D"), intCol("Number", 1, 2, 3, 4))
t2 = newTable(stringCol("Letter", "C", "B", "D", "A"), intCol("Number", 6, 7, 8, 9))
t3 = newTable(stringCol("Letter", "D", "C", "A", "B"), intCol("Number", 9, 8, 7, 6))

// create a MultiJoinTable object and join the three tables
MultiJoinTable mtTable = MultiJoinFactory.of("Letter, Number", t1, t2, t3)

// access the multijoin object's internal table
result = mtTable.table()

// access the multijoin object's key columns
println mtTable.keyColumns()
```

## Related documentation

- [`newTable`](../create/newTable.md)
- [Multi-Join](./multijoin.md)
