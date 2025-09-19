---
title: Use the ternary conditional operator in query strings
sidebar_label: Ternary-if
---

This guide will show you how to use the ternary conditional operator, also known as ternary-if, in query strings. The operator evaluates a boolean expression and returns the result of one of two expressions, depending on whether the boolean expression evaluates to true or false. It is similar to an inline `if-then-else` code block.

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

```python order=source,result
from deephaven import new_table

from deephaven.column import string_col, int_col, double_col

source = new_table(
    [
        string_col(
            "Type", ["Pine", "Fir", "Cedar", "Oak", "Ash", "Walnut", "Beech", "Cherry"]
        ),
        double_col("Price", [1.95, 3.70, 3.25, 3.45, 4.25, 7.95, 4.10, 5.25]),
    ]
)

result = source.update(formulas=["Budget = (Price <= 3.50) ? `yes` : `no` "])
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

```python test-set=1 order=result,woods
from deephaven import new_table
from deephaven.column import string_col, double_col

woods = new_table(
    [
        string_col(
            "Type", ["Pine", "Fir", "Cedar", "Oak", "Ash", "Walnut", "Beech", "Cherry"]
        ),
        string_col(
            "Hardness", ["soft", "soft", "soft", "hard", "hard", "hard", "hard", "hard"]
        ),
        double_col("Price", [1.95, 2.50, 3.25, 3.45, 4.25, 7.95, 4.10, 5.25]),
    ]
)

result = woods.update(
    formulas=[
        "Possible = (Hardness == `hard`) ? ( (Price <= 3.50) ? `yes` : `no` ) : `no` "
    ]
)
```

If hardwood is a requirement but the budget is flexible, nested ternary-ifs can be used to categorize hardwoods as `budget` or `expensive` and all other wood types as `no`. This is seen below:

```python test-set=1
result = woods.update(
    formulas=[
        "possible = (Hardness == `hard`) ? ( (Price <= 3.50) ? `budget` : `expensive` ) : `no` "
    ]
)
```

## Functions in ternary conditional operators

Using a function in a ternary statement is very straightforward. All you need to do is replace the `condition` with the function call. The method _must_ return a boolean value. If it's a [Python function](./python-functions.md), be sure to include a type hint or typecast to ensure the output value is a boolean.

The following example uses a function in a ternary operator to determine if the wood flooring options are in the budget.

```python test-set=1
def budget(price) -> bool:
    return price <= 3.50


result = woods.update("Budget = budget(Price) ? `yes` : `no` ")
```

## Related documentation

- [Operators](./operators.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`update`](../reference/table-operations/select/update.md)
