---
title: Use the ternary conditional operator in query strings
sidebar_label: Ternary-if
---

This guide will show you how to use Groovy's native ternary operator (`condition ? valueIfTrue : valueIfFalse`), also known as ternary-if, in query strings. The operator evaluates a boolean expression and returns the result of one of two expressions, depending on whether the boolean expression evaluates to true or false. It is similar to an inline `if-then-else` code block.

## Ternary conditional operator (ternary-if)

The syntax for the ternary conditional operator is:
`condition ? expressionIfTrue : expressionIfFalse`

The question mark (`?`) separates the condition from the expressions, and the colon (`:`) separates the expression evaluated when the condition is true from the expression evaluated when the condition is false.

The expression `x ? y : z` evaluates as follows:

- If `x` is true, the expression evaluates to `y`.
- If `x` is false, the expression evaluates to `z`.

The expression `x ? (y ? 1 : 2) : 3` evaluates as follows:

- If both `x` and `y` are true, the expression evaluates to 1.
- If `x` is true, and `y` is false, the expression evaluates to 2.
- If `x` is false, the expression evaluates to 3.

In the following example, a new column, `Budget`, is created. The column contains `yes` if the value in the `Price` column is less than or equal to 3.50 and `no` otherwise.

```groovy order=woods,result
woods = newTable(
    stringCol("Type", "Pine", "Fir", "Cedar", "Oak", "Ash", "Walnut", "Beech", "Cherry"),
    doubleCol("Price", 1.95, 2.50, 3.25, 3.45, 4.25, 7.95, 4.10, 5.25)
)

result = woods.update("Budget = (Price <= 3.50) ? `yes` : `no`")
```

## Nested ternary conditional operators

For more complex cases with multiple conditions, ternary-if statements can be nested. For example:

`condition1 ? (condition2 ? value1 : value2) : value3`

The expression `x ? (y ? 1 : 2) : 3` evaluates as follows:

- If both `x` and `y` are true, the expression evaluates to 1.
- If `x` is true, and `y` is false, the expression evaluates to 2.
- If `x` is false, the expression evaluates to 3.

Consider a home builder with a budget of $3.50/board-foot to purchase hardwood lumber. The builder would like to know what types of wood are within their budget. Nested ternary operators can create `yes` and `no` values based on whether or not a type of wood meets this requirement.

In this example, the `Possible` column evaluates to `yes` only if the wood is both hardwood and the price is less than $3.50. Otherwise, the wood is not offered as a possibility for the customer.

```groovy test-set=1 order=woods,result
woods = newTable(
    stringCol("Type", "Pine", "Fir", "Cedar", "Oak", "Ash", "Walnut", "Beech", "Cherry"),
    stringCol("Hardness", "soft", "soft", "soft", "hard", "hard", "hard", "hard", "hard"),
    doubleCol("Price", 1.95, 2.50, 3.25, 3.45, 4.25, 7.95, 4.10, 5.25)
)

result = woods.update("Possible = (Hardness == `hard`) ? ((Price <= 3.50) ? `yes` : `no`) : `no`")
```

If hardwood is a requirement but the budget is flexible, nested ternary-ifs can be used to categorize hardwoods as `budget` or `expensive` and all other wood types as `no`. This is seen below:

```groovy test-set=1
result = woods.update("possible = (Hardness == `hard`) ? ((Price <= 3.50) ? `budget` : `expensive`) : `no`")
```

## Using custom methods

Using a function in a ternary statement is very straightforward. All you need to do is replace the `condition` with the function call. The method _must_ return a `(Boolean)` value.

The following example uses a function in a ternary operator to determine if the wood flooring options are in the budget.

```groovy test-set=1
budget = { price -> price <= 3.50 }

result = woods.update("Budget = (Boolean)budget(Price) ? `yes` : `no`")
```

## Related documentation

- [Operators](./operators.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [Query string overview](./query-string-overview.md)
