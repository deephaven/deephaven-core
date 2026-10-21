---
title: Use the ternary conditional operator in query strings
---

This guide shows you how to use the ternary conditional operator, also known as ternary-if, in [query strings](./query-string-overview.md). The operator evaluates a boolean expression and returns the result of one of two expressions, depending on whether the boolean expression evaluates to true or false. It works like an `if`/`else` statement written as a single expression.

## Syntax

The syntax for the ternary conditional operator is:

`condition ? expressionIfTrue : expressionIfFalse`

The question mark (`?`) follows the condition, and the colon (`:`) separates the two possible results. Parentheses around the condition are optional, but they make a [formula](./formulas.md) easier to read, especially when ternary-ifs are nested.

The expression `x ? y : z` evaluates as follows:

- If `x` is true, the expression evaluates to `y`.
- If `x` is false, the expression evaluates to `z`.

Consider a home builder with a budget of $3.50 per board-foot for lumber. The builder wants to know which types of wood are within budget. The following example adds a `Budget` column that is `yes` when `Price` is less than or equal to $3.50 and `no` otherwise. The backticks around `yes` and `no` mark them as [string literals](./string-char-literals.md) in the [query string](./query-string-overview.md).

```groovy test-set=1 order=woods,result
woods = newTable(
    stringCol("Type", "Pine", "Fir", "Cedar", "Oak", "Ash", "Walnut", "Beech", "Cherry"),
    stringCol("Hardness", "soft", "soft", "soft", "hard", "hard", "hard", "hard", "hard"),
    doubleCol("Price", 1.95, 2.50, 3.25, 3.45, 4.25, 7.95, 4.10, 5.25)
)

result = woods.update("Budget = (Price <= 3.50) ? `yes` : `no`")
```

## Nested ternary conditional operators

To handle multiple conditions, nest one ternary-if inside another:

`condition1 ? (condition2 ? value1 : value2) : value3`

The expression `x ? (y ? 1 : 2) : 3` evaluates as follows:

- If both `x` and `y` are true, the expression evaluates to `1`.
- If `x` is true and `y` is false, the expression evaluates to `2`.
- If `x` is false, the expression evaluates to `3`.

Now suppose the builder needs hardwood as well as a price within budget. In the following example, the `Possible` column is `yes` only if the wood is hardwood and its price is less than or equal to $3.50, and `no` otherwise.

```groovy test-set=1 order=result
result = woods.update("Possible = (Hardness == `hard`) ? ((Price <= 3.50) ? `yes` : `no`) : `no`")
```

Both `no` branches return the same value, so this [formula](./formulas.md) gives the same result as one condition that joins the two tests with the [logical AND operator](./operators.md#logical-operators) (`&&`): `` (Hardness == `hard` && Price <= 3.50) ? `yes` : `no` ``. Nesting pays off when each branch returns a different value, as in the next example.

If hardwood is a requirement but the budget is flexible, use nested ternary-ifs to create a `Category` column that labels hardwoods `budget` or `expensive` and all other wood types `no`:

```groovy test-set=1
result = woods.update("Category = (Hardness == `hard`) ? ((Price <= 3.50) ? `budget` : `expensive`) : `no`")
```

## Return values of different types

Both branches of a ternary-if must return compatible types, because the expression has a single result type. Two cases follow from this rule:

- If neither branch type can be converted to the other, such as a number in one branch and a [string](../reference/query-language/types/strings.md) in the other, the query fails with an incompatible types error.
- If one branch is `null` and the other returns a numeric, `char`, or `boolean` type, the `null` becomes that type's [null value](./null-inf-nan.md), such as [`NULL_DOUBLE`](./built-in-constants.md#null-constants).

The following example keeps the prices that are within budget and puts a [null](./null-inf-nan.md) `double` in the rows whose price is over budget:

```groovy test-set=1
result = woods.update("BudgetPrice = (Price <= 3.50) ? Price : null")
```

## Use a closure as the condition

To use a [Groovy closure](./groovy-closures.md) as the condition in a ternary-if, replace the condition with a call to the closure. The condition must evaluate to a boolean, but a closure called from a [query string](./query-string-overview.md) always returns [`Object`](../reference/query-language/types/objects.md). [Cast](./casting.md#groovy-closures) the call with `(Boolean)` in the [formula](./formulas.md).

The following example uses a [Groovy closure](./groovy-closures.md) as the ternary-if condition to determine which types of wood are within the budget.

```groovy test-set=1
budget = { price -> price <= 3.50 }

result = woods.update("Budget = (Boolean)budget(Price) ? `yes` : `no`")
```

## Related documentation

- [Operators](./operators.md)
- [Query string overview](./query-string-overview.md)
- [Formulas](./formulas.md)
- [String and char literals](./string-char-literals.md)
- [Groovy closures in queries](./groovy-closures.md)
- [Casting](./casting.md)
- [Null, NaN, and infinity values](./null-inf-nan.md)
- [Built-in constants](./built-in-constants.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
- [`stringCol`](../reference/table-operations/create/stringCol.md)
- [`doubleCol`](../reference/table-operations/create/doubleCol.md)
- [`update`](../reference/table-operations/select/update.md)
- [`newTable` Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#newTable(io.deephaven.engine.table.impl.util.ColumnHolder...))
- [`update` Javadoc](/core/javadoc/io/deephaven/api/TableOperations.html#update(java.lang.String...))
