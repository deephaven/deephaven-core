---
title: Formulas
---

This guide covers formulas in the context of [query strings](./query-string-overview.md). Formulas in Deephaven work like formulas in math and computer science. In a query string, a formula is an expression that defines the values of a column based on other columns, constants, functions, and more.

A formula in a query string creates a new column in a table, or replaces an existing column that has the same name. Formulas work on both [static](../conceptual/table-types.md#static-tables) and [real-time tables](../conceptual/table-types.md). When a [real-time table](../conceptual/table-types.md) updates, the columns its formulas create update with it.

On a [real-time table](../conceptual/table-types.md), the engine rejects some formulas that use the [built-in variables](./built-in-variables.md) `i`, `ii`, and `k`, or a [column array variable](./work-with-arrays.md#using-the-underscore-operator) such as `Column_`. [Refreshing table restrictions](./built-in-variables.md#refreshing-table-restrictions) lists the cases that are safe.

The following table operations use formulas to create columns:

- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)
- [`update`](../reference/table-operations/select/update.md)
- [`updateView`](../reference/table-operations/select/update-view.md)
- [`lazyUpdate`](../reference/table-operations/select/lazy-update.md)
- [`selectDistinct`](../reference/table-operations/select/select-distinct.md)
- [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) with [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md)
- [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) with [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md)

## What makes up a formula?

Every formula has three parts, in this order:

1. The left-hand side names the output column.
2. The assignment operator separates the two sides.
3. The right-hand side is the expression that computes each row's value.

### The left-hand side (LHS)

The LHS of a formula contains only the name of the column the formula creates or replaces. The name must follow these rules:

- It must start with an ASCII letter (`A`–`Z` or `a`–`z`), an underscore (`_`), or a dollar sign (`$`).
- After the first character, it can contain only ASCII letters, digits, underscores, and dollar signs. That rules out spaces, symbols such as `@`, `#`, `%`, `+`, `-`, and `=`, and non-ASCII characters such as `é` or `€`, even though [Java identifiers](https://docs.oracle.com/javase/specs/jls/se17/html/jls-3.html#jls-3.8) can contain non-ASCII characters.
- It cannot be a Java [reserved keyword](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/_keywords.html) or literal (`true`, `false`, `null`).
- It cannot be [`in`](../reference/query-language/match-filters/in.md), `not`, or one of the [built-in variables](./built-in-variables.md) `i`, `ii`, and `k`. The query language reserves those variables for row positions and row keys.

Deephaven recommends `PascalCase`, or upper [camel case](https://en.wikipedia.org/wiki/Camel_case), for column names.

### The assignment operator

Every formula _must_ contain the assignment operator (`=`) to assign the value of the RHS to the LHS column. Don't confuse it with the [equality operator](./operators.md#comparison-operators) (`==`), which compares two values. Only the first `=` in a formula assigns. Any later single `=` on the RHS is an equality test, the same as `==`.

### The right-hand side (RHS)

The RHS of a formula can be any valid expression that evaluates to a value. It can include:

- Columns, referenced by name
- Built-ins
  - [Constants](./built-in-constants.md)
  - [Functions](./built-in-functions.md)
  - [Variables](./built-in-variables.md)
- Literals
  - [Boolean and numeric](./boolean-numeric-literals.md)
  - [String and char](./string-char-literals.md)
  - [Date-time](./date-time-literals.md)
- Groovy
  - [Variables](./groovy-variables.md)
  - [Closures](./groovy-closures.md)
  - [Classes](./groovy-classes.md)
- [Java classes and objects](./java-classes.md)
- [Operators](./operators.md)

## A simple formula

Consider the following formula:

```text
Fahrenheit = (Celsius * 9 / 5) + 32
```

You may recognize this formula: it converts a temperature in Celsius to Fahrenheit. From left to right, its parts are:

- `Fahrenheit` is the name of the output column.
- `=` is the [assignment operator](#the-assignment-operator).
- `(` and `)` are parentheses used to group operations.
- `Celsius` is the name of another column.
- `*` is the [multiplication operator](./operators.md#arithmetic-operators).
- `9` is a [numeric literal](./boolean-numeric-literals.md#numeric-literals).
- `/` is the [division operator](./operators.md#arithmetic-operators).
- `5` is another [numeric literal](./boolean-numeric-literals.md#numeric-literals).
- `+` is the [addition operator](./operators.md#arithmetic-operators).
- `32` is another [numeric literal](./boolean-numeric-literals.md#numeric-literals).

The following example uses the formula in [`updateView`](../reference/table-operations/select/update-view.md) to add a `Fahrenheit` column to a new table called `result`:

```groovy order=result,source
source = emptyTable(10).update("Celsius = randomInt(0, 100)")
result = source.updateView("Fahrenheit = (Celsius * 9 / 5) + 32")
```

## Several formulas in one operation

One table operation can take several formulas. Deephaven evaluates them in order, so a formula can use a column that an earlier formula in the same call created. A formula whose LHS names an existing column replaces that column.

The following example passes three formulas to [`update`](../reference/table-operations/select/update.md). The second formula uses the `Celsius` column that the first formula creates. The third formula replaces the `Fahrenheit` column with its value rounded to the nearest whole number:

```groovy order=result
result = emptyTable(5).update(
    "Celsius = i * 7.5",
    "Fahrenheit = (Celsius * 9 / 5) + 32",
    "Fahrenheit = round(Fahrenheit)",
)
```

## A more complex formula

Consider the following formula:

```text
RandomIsPositive = (randomDouble(-1, 1) > 0) ? `Positive` : `Negative`
```

This formula uses the [ternary conditional operator](./ternary-if-how-to.md) (`?`) to determine the value of the `RandomIsPositive` column based on the value of a random double. Its parts are:

- `RandomIsPositive` is the name of the output column.
- `=` is the [assignment operator](#the-assignment-operator).
- `(` and `)` are parentheses used to group operations.
- `randomDouble(-1, 1)` is a call to the built-in [`randomDouble`](../reference/query-language/query-library/auto-imported/math.md) function, which generates a random double value from -1 (inclusive) to 1 (exclusive).
- `>` is the [greater-than comparison operator](./operators.md#comparison-operators).
- `0` is a [numeric literal](./boolean-numeric-literals.md#numeric-literals).
- `?` is the [ternary conditional operator](./ternary-if-how-to.md).
- `` `Positive` `` is a [string literal](./string-char-literals.md) (note the backticks).
- `:` is the separator between the value used when the condition is true and the value used when it is false.
- `` `Negative` `` is another [string literal](./string-char-literals.md) (note the backticks).

The following example uses the formula in [`update`](../reference/table-operations/select/update.md). Each row is `Positive` or `Negative`, depending on the random value generated for that row:

```groovy order=result
result = emptyTable(10).update("RandomIsPositive = (randomDouble(-1, 1) > 0) ? `Positive` : `Negative`")
```

## Strings that look like formulas but aren't

[Filters](./filters.md), joins, and most aggregations also take strings that contain `=`. These strings look like formulas, but they aren't:

- In [filters](./filters.md), such as those passed to [`where`](../reference/table-operations/filter/where.md), the string is a boolean expression that decides which rows to keep. In a filter, a single [`=`](../reference/query-language/match-filters/equals.md) is an equality test, the same as [`==`](../reference/query-language/match-filters/equals.md), not an assignment. For example, `X = 5` keeps the rows where `X` is 5.
- In [exact and relational joins](./joins-exact-relational.md) and [time-series and range joins](./joins-timeseries-range.md), both sides of `A = B` must be column names. In the match columns, `A = B` matches left-table column `A` to right-table column `B`. In the columns to add, `A = B` adds right-table column `B` to the result under the name `A`. Most join methods take the match columns as the `columnsToMatch` argument and the columns to add as the `columnsToAdd` argument.
- In [combined aggregations](./combined-aggregations.md) and [rolling aggregations](./rolling-aggregations.md), operations that compute a fixed statistic, such as [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md) and [`RollingSum`](../reference/table-operations/update-by-operations/rolling-sum.md), take `A = B` strings. Both sides must be column names: `A` is the output column, and `B` is the input column it aggregates.

To aggregate the result of an expression, use [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md). For a rolling window, use [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md). Each of these takes a full formula.

For example, the following query tries to sum the product of two columns with [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md). It fails with `InvalidNameException: Invalid column name "Price * Qty"`, because the RHS of an [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md) string must be a column name:

```groovy should-fail
source = newTable(
    stringCol("Sym", "A", "A", "B", "B"),
    doubleCol("Price", 10.0, 11.0, 20.0, 21.0),
    intCol("Qty", 1, 2, 3, 4),
)

result = source.aggBy(AggSum("Total = Price * Qty"), "Sym")
```

[`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md) takes a full formula, so it can aggregate the expression:

```groovy order=result,source
source = newTable(
    stringCol("Sym", "A", "A", "B", "B"),
    doubleCol("Price", 10.0, 11.0, 20.0, 21.0),
    intCol("Qty", 1, 2, 3, 4),
)

result = source.aggBy(AggFormula("Total = sum(Price * Qty)"), "Sym")
```

## Formulas on partitioned tables

The table operations that take formulas, such as [`update`](../reference/table-operations/select/update.md) and [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md), work on ordinary tables. To apply one of them to every constituent table of a [partitioned table](./partitioned-tables.md), use the partitioned table's [proxy](./partitioned-tables.md#proxy) or [`transform`](../reference/table-operations/partitioned-tables/transform.md):

- The [proxy](../reference/table-operations/partitioned-tables/proxy.md) supports every operation listed at the top of this page.
- A [`transform`](../reference/table-operations/partitioned-tables/transform.md) can apply any of them.

## Related documentation

- [Deephaven's table types](../conceptual/table-types.md)
- [Query strings](./query-string-overview.md)
- [Select and update columns](./use-select-view-update.md)
- [Operators](./operators.md)
- [Filters](./filters.md)
- [Ternary conditional operator](./ternary-if-how-to.md)
- [Partitioned tables](./partitioned-tables.md)
- [Exact and relational joins](./joins-exact-relational.md)
- [Time-series and range joins](./joins-timeseries-range.md)
- [Combined aggregations](./combined-aggregations.md)
- [Rolling aggregations](./rolling-aggregations.md)
- [Built-in constants](./built-in-constants.md)
- [Built-in functions](./built-in-functions.md)
- [Built-in variables](./built-in-variables.md)
- [Arrays](./work-with-arrays.md)
- [Boolean and numeric literals](./boolean-numeric-literals.md)
- [String and char literals](./string-char-literals.md)
- [Date-time literals](./date-time-literals.md)
- [Groovy variables](./groovy-variables.md)
- [Groovy closures in queries](./groovy-closures.md)
- [Groovy classes](./groovy-classes.md)
- [Java classes](./java-classes.md)
- [`AggFormula`](../reference/table-operations/group-and-aggregate/AggFormula.md)
- [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md)
- [Math functions](../reference/query-language/query-library/auto-imported/math.md)
- [`doubleCol`](../reference/table-operations/create/doubleCol.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`intCol`](../reference/table-operations/create/intCol.md)
- [`lazyUpdate`](../reference/table-operations/select/lazy-update.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
- [`proxy`](../reference/table-operations/partitioned-tables/proxy.md)
- [`RollingFormula`](../reference/table-operations/update-by-operations/rolling-formula.md)
- [`RollingSum`](../reference/table-operations/update-by-operations/rolling-sum.md)
- [`select`](../reference/table-operations/select/select.md)
- [`selectDistinct`](../reference/table-operations/select/select-distinct.md)
- [`stringCol`](../reference/table-operations/create/stringCol.md)
- [`transform`](../reference/table-operations/partitioned-tables/transform.md)
- [`update`](../reference/table-operations/select/update.md)
- [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md)
- [`updateView`](../reference/table-operations/select/update-view.md)
- [`view`](../reference/table-operations/select/view.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`Table` Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
- [`Aggregation` Javadoc](/core/javadoc/io/deephaven/api/agg/Aggregation.html)
- [`UpdateByOperation` Javadoc](/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html)
