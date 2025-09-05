---
title: Formulas
---

This guide covers formulas in the context of [query strings](./query-string-overview.md). Formulas in Deephaven are no different than formulas in math and computer science - they are expressions that define the value of a variable based on other variables, constants, functions, and more.

A formula in a query string creates a new column in a table. If the resultant column name is the same as an existing column name, the existing column is replaced with the new column. Formulas always assign the right-hand-side (RHS) to the left-hand-side (LHS) using the assignment operator (`=`). The LHS is the name of the column that will be created or updated, and the RHS defines the values that will be assigned to that column.

Formulas are used in the following table operations to create new columns:

- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)
- [`update`](../reference/table-operations/select/update.md)
- [`update_view`](../reference/table-operations/select/update-view.md)
- [`lazy_update`](../reference/table-operations/select/lazy-update.md)

In joins, formulas are used to define the columns to join the data on, and in aggregations, formulas define the output column names:

- Joins
  - [Exact and relational](./joins-exact-relational.md)
  - [Time-series and range](./joins-timeseries-range.md)
- Aggregations
  - [Dedicated](./dedicated-aggregations.md)
  - [Combined](./combined-aggregations.md)
  - [Rolling](./rolling-aggregations.md)

Additionally, formulas can be used in [partitioned table](./partitioned-tables.md) operations.

## What comprises a formula?

### The left-hand-side (LHS)

The LHS of a formula contains only the name of the column that will be created or overwritten if the column name is already present. It must be a valid column name, which means it must follow these rules:

- Start with a letter (`A-Z` or `a-z`) or an underscore (`_`).
- Cannot contain any spaces.
- Cannot contain any special characters (e.g., `@`, `#`, `$`, `%`, etc.).
- Cannot contain any arithmetic characters (`+`, `-`, `=`, etc.).
- Cannot equal any [reserved keywords](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/_keywords.html).

Generally speaking, Deephaven recommends using `PascalCase` (upper [camel case](https://en.wikipedia.org/wiki/Camel_case)) naming convention for column names.

### The assignment operator

Every formula _must_ contain the assignment operator (`=`) to assign the value of the RHS to the LHS column. The assignment operator is used to indicate that the value on the right-hand-side will be assigned to the column on the left-hand-side.

### The right-hand side (RHS)

The RHS of a formula can be any valid expression that evaluates to a value. It can include (in no particular order):

- Built-ins
  - [Constants](./built-in-constants.md)
  - [Functions](./built-in-functions.md)
  - [Variables](./built-in-variables.md)
- Literals
  - [Boolean and numeric](./boolean-numeric-literals.md)
  - [String and char](./string-char-literals.md)
  - [Date-time](./date-time-literals.md)
- Python
  - [Variables](./python-variables.md)
  - [Functions](./python-functions.md)
  - [Classes](./python-classes.md)
- [Operators](./operators.md)

## A simple formula

Consider the following formula:

```text
Fahrenheit = (Celsius * 9 / 5) + 32
```

You may recognize this formula—it calculates the temperature in Fahrenheit given a temperature in Celsius. Going from left to right, the formula can be broken down as follows:

- `Fahrenheit` is the name of the resultant column.
- `(` and `)` are parentheses used to group operations.
- `=` is the equality operator.
- `Celsius` is the name of another column.
- `9` is a numeric literal.
- `/` is the division operator.
- `5` is another numeric literal.
- `+` is the addition operator.
- `32` is another numeric literal.

Here's an example of the formula being used to create a new column in a table called `result`:

```python order=result,source
from deephaven import empty_table

source = empty_table(10).update("Celsius = randomInt(0, 100)")
result = source.update_view("Fahrenheit = (Celsius * 9 / 5) + 32")
```

## A more complex formula

Consider the following formula:

```text
RandomIsPositive = (randomDouble(-1, 1) > 0) ? `Positive` : `Negative`
```

This formula uses [ternary conditional operator](./ternary-if-how-to.md) (`?`) to determine the value of the `RandomIsPositive` column based on the value of a random double. The formula can be broken down as follows:

- `RandomIsPositive` is the name of the resultant column.
- `=` is the assignment operator.
- `(` and `)` are parentheses used to group operations.
- `randomDouble(-1, 1)` is a function call that generates a random double value between -1 and 1.
- `>` is the greater-than comparison operator.
- `0` is a numeric literal.
- `?` is the ternary conditional operator.
- `` `Positive` `` is a [string literal](./string-char-literals.md) (note the backticks).
- `:` is the ternary conditional operator's "else" clause.
- `` `Negative` `` is another [string literal](./string-char-literals.md) (note the backticks).

## Related documentation

- [Query strings](./query-string-overview.md)
- [Select and update columns](./use-select-view-update.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
