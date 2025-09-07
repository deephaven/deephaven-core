---
title: Use the ternary conditional operator in query strings
sidebar_label: Ternary-if
---

This guide will show you how to use the ternary conditional operator, also known as ternary-if, in query strings. The ternary conditional operator evaluates a boolean expression and returns the result of one of two expressions, depending on whether the boolean expression evaluates to true or false. The operator is similar to an inline `if-then-else` code block.

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

### Example

A home builder has a budget of $3.50/board-foot to purchase lumber and would like to know what types of wood are within their budget. We can use the ternary operator to create a new table with a "yes" or "no" column that answers if a wood type is within the budget constraints.

```groovy test-set=1 order=woods,result
woods = newTable(
    stringCol("Type", "Pine", "Fir", "Cedar", "Oak", "Ash", "Walnut", "Beech", "Cherry"),
    stringCol("Hardness", "soft", "soft", "soft", "hard", "hard", "hard", "hard", "hard"),
    doubleCol("Price", 1.95, 2.50, 3.25, 3.45, 4.25, 7.95 , 4.10, 5.25)
)

result = woods.update("Budget = (Price<=3.50) ? `yes` : `no` ")
```

## Nested ternary conditional operators

For more complex cases with multiple conditions, ternary-if statements can be nested. For example:

`condition1 ? (condition2 ? value1 : value2) : value3`

The expression `x ? (y ? 1 : 2) : 3` evaluates as follows:

- If both `x` and `y` are true, the expression evaluates to 1.
- If `x` is true, and `y` is false, the expression evaluates to 2.
- If `x` is false, the expression evaluates to 3.

### Example

A home builder has a budget of $3.50/board-foot to purchase hardwood lumber and would like to know what types of wood are within their budget. We can use nested ternary operators to create a new table with a `yes` or `no` column that answers if a wood type is both within the budget constraints and a hardwood.

In this example, the `Possible` column evaluates to `yes` only if the wood is both hardwood and the price is less than $3.50. Otherwise, the wood is not offered as a possibility for the customer.

```groovy test-set=1
result = woods.update("Possible = (Hardness == `hard`) ? ( (Price <= 3.50) ? `yes` : `no` ) : `no` ")
```

If hardwood is a requirement but the budget is flexible, nested ternary-ifs can be used to categorize hardwoods as `budget` or `expensive` and all other wood types as `no`. This is seen below:

```groovy test-set=1
result = woods.update("possible = (Hardness == `hard`) ? ( (Price <= 3.50) ? `budget` : `expensive` ) : `no` ")
```

## Using a custom method

Using a custom method in a ternary statement is very straightforward. All you need to do is replace the `condition` with your method call. However, there's a catch - you need to cast the method call to `(Boolean)`.

The following example shows how to use a custom method in a ternary statement.

```groovy order=woods,result
budget = { price ->
    return price <= 3.50
}

woods = newTable(
    stringCol("Type", "Pine", "Fir", "Cedar", "Oak", "Ash", "Walnut", "Beech", "Cherry"),
    stringCol("Hardness", "soft", "soft", "soft", "hard", "hard", "hard", "hard", "hard"),
    doubleCol("Price", 1.95, 2.50, 3.25, 3.45, 4.25, 7.95 , 4.10, 5.25)
)

result = woods.update("Budget = (Boolean)budget(Price) ? `yes` : `no` ")
```

## Related documentation

[Create a new table](./new-and-empty-table.md#newtable)
