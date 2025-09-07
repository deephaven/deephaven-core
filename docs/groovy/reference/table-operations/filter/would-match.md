---
title: wouldMatch
---

The `wouldMatch` method evaluates each row of an input table according to a user-specified formula and returns the table with an additional column indicating which rows would match the selection criteria.

## Syntax

```
table.wouldMatch(expressions...)
table.wouldMatch(matchers...)
```

## Parameters

<ParamTable>
<Param name="expressions" type="String...">

One or more strings, each of which performs an assignment and a truth check. For example:

- `NewColumnName = ExistingColumnName == 3`
- `XMoreThan5 = X > 5`

</Param>
<Param name="matchers" type="WouldMatchPair">

A pair of either a column name and a filter, or a column name and a String expression.

</Param>
</ParamTable>

## Returns

A new table with an additional Boolean column, with values indicating whether or not each row would match the provided formula's criteria.

## Examples

The following example returns a copy of the source table, with an additional column showing which rows match based on whether the value in the `Number` column is even.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "B", "C", "D", "E", "A", "B", "C", "D", "E"),
    intCol("Number", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
)

result = source.wouldMatch("Matches = (Number % 2) == 0")
```

The following example returns a table with a new column that shows whether or not the String values in the `Letter` column match "B".

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "B", "C", "D", "E", "A", "B", "C", "D", "E"),
    intCol("Number", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
)
result = source.wouldMatch("Matches = Letter == `B`")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [`where`](./where.md)
- [`WouldMatchPair` Javadoc](/core/javadoc/io/deephaven/engine/table/WouldMatchPair.html)
- [`wouldMatch` Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#wouldMatch(java.lang.String...))
