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

```python test-set=1 order=woods,result
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

result = woods.update(formulas=["Budget = (Price <= 3.50) ? `yes` : `no`"])
```

## Nested ternary conditional operators

To handle multiple conditions, nest one ternary-if inside another:

`condition1 ? (condition2 ? value1 : value2) : value3`

The expression `x ? (y ? 1 : 2) : 3` evaluates as follows:

- If both `x` and `y` are true, the expression evaluates to `1`.
- If `x` is true and `y` is false, the expression evaluates to `2`.
- If `x` is false, the expression evaluates to `3`.

Now suppose the builder needs hardwood as well as a price within budget. In the following example, the `Possible` column is `yes` only if the wood is hardwood and its price is less than or equal to $3.50, and `no` otherwise.

```python test-set=1 order=result
result = woods.update(
    formulas=[
        "Possible = (Hardness == `hard`) ? ((Price <= 3.50) ? `yes` : `no`) : `no`"
    ]
)
```

Both `no` branches return the same value, so this [formula](./formulas.md) gives the same result as one condition that joins the two tests with the [logical AND operator](./operators.md#logical-operators) (`&&`): `` (Hardness == `hard` && Price <= 3.50) ? `yes` : `no` ``. Nesting pays off when each branch returns a different value, as in the next example.

If hardwood is a requirement but the budget is flexible, use nested ternary-ifs to create a `Category` column that labels hardwoods `budget` or `expensive` and all other wood types `no`:

```python test-set=1
result = woods.update(
    formulas=[
        "Category = (Hardness == `hard`) ? ((Price <= 3.50) ? `budget` : `expensive`) : `no`"
    ]
)
```

## Return values of different types

Both branches of a ternary-if must return compatible types, because the expression has a single result type. Two cases follow from this rule:

- If neither branch type can be converted to the other, such as a number in one branch and a [string](../reference/query-language/types/strings.md) in the other, the query fails with an incompatible types error.
- If one branch is `null` and the other returns a numeric, `char`, or `boolean` type, the `null` becomes that type's [null value](./null-inf-nan.md), such as [`NULL_DOUBLE`](./built-in-constants.md#null-constants).

The following example keeps the prices that are within budget and puts a [null](./null-inf-nan.md) `double` in the rows whose price is over budget:

```python test-set=1
result = woods.update(formulas=["BudgetPrice = (Price <= 3.50) ? Price : null"])
```

## Use a function as the condition

To use a [Python function](./python-functions.md) as the condition in a ternary-if, replace the condition with a call to the function. The condition must evaluate to a boolean, so the function needs one of the following:

- A return [type hint](./python-functions.md#type-hints) such as `-> bool`.
- A `(Boolean)` [cast](./casting.md#python-functions) around the call in the [query string](./query-string-overview.md).

Without either one, the function returns a [`PyObject`](./pyobjects.md) to the [query string](./query-string-overview.md), even when it returns `True` or `False`.

The following example uses a [Python function](./python-functions.md) as the ternary-if condition to determine which types of wood are within the budget.

```python test-set=1
def budget(price) -> bool:
    return price <= 3.50


result = woods.update(formulas=["Budget = budget(Price) ? `yes` : `no`"])
```

If you can't add a [type hint](./python-functions.md#type-hints), [cast](./casting.md#python-functions) the call with `(Boolean)` instead. The following example gives the same `Budget` column as the previous one:

```python test-set=1
def budget_untyped(price):
    return price <= 3.50


result = woods.update(
    formulas=["Budget = (Boolean)budget_untyped(Price) ? `yes` : `no`"]
)
```

## Related documentation

- [Operators](./operators.md)
- [Query string overview](./query-string-overview.md)
- [Formulas](./formulas.md)
- [String and char literals](./string-char-literals.md)
- [Python functions](./python-functions.md)
- [Casting](./casting.md)
- [Null, NaN, and infinity values](./null-inf-nan.md)
- [Built-in constants](./built-in-constants.md)
- [Handle PyObjects in tables](./pyobjects.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`string_col`](../reference/table-operations/create/stringCol.md)
- [`double_col`](../reference/table-operations/create/doubleCol.md)
- [`update`](../reference/table-operations/select/update.md)
- [`new_table` Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.new_table)
- [`update` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.update)
