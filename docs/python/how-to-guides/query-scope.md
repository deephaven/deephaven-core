---
title: Query scope
---

Deephaven [query strings](./query-string-overview.md) let you express complex queries in a concise syntax. These [query strings](./query-string-overview.md) can implement [formulas](./formulas.md) or [filters](./filters.md) that use [Python variables](./python-variables.md). For example, the [query string](./query-string-overview.md) in this [`update`](../reference/table-operations/select/update.md) uses the variable `a` from Python. Variables make queries easier to understand and reuse.

```python skip-test
a = 2
t2 = t.update(["Y = a * X"])
```

The query scope is the set of user-defined variables and functions that a [query string](./query-string-overview.md) can refer to. When Deephaven evaluates `Y = a * X`, it looks up `a` in the query scope. This guide walks you through how Deephaven resolves [Python variables](./python-variables.md) and [Python functions](./python-functions.md) in [query strings](./query-string-overview.md). To learn how to use each kind of variable and function, see [Python variables in query strings](./python-variables.md) and [Python functions in query strings](./python-functions.md).

> [!NOTE]
> Variable names and function names are case-sensitive.

## Query scope in Python

When the Deephaven engine compiles a [query string](./query-string-overview.md), it must resolve every variable and function the string uses. Python's [LEGB](https://realpython.com/python-scope-legb-rule/) rule looks up a name in the local, enclosing, global, and built-in scopes, in that order. The Deephaven query scope follows similar rules, with some important distinctions.

When a [query string](./query-string-overview.md) uses a name, the engine resolves it in this order:

1. A column, a [column array](./work-with-arrays.md#using-the-underscore-operator) such as `A_`, or a [built-in variable](./built-in-variables.md) ([`i`](./built-in-variables.md), [`ii`](./built-in-variables.md), [`k`](./built-in-variables.md)).
2. A variable in the local (function) scope.
3. A variable in the global (module) scope.
4. A [built-in constant](./built-in-constants.md). A variable named `MINUTE` therefore hides the built-in [`MINUTE`](./built-in-constants.md).

A variable name that isn't found in any of these places fails with a `Cannot find variable or class` error.

Function calls resolve the other way around: a [built-in function](./built-in-functions.md) wins over a [Python function](./python-functions.md) with the same name when one of its overloads accepts the arguments.

Deephaven does not search the enclosing (nonlocal) scope directly. A [query string](./query-string-overview.md) can use an enclosing function's variable only if the inner function's own code also refers to it, for example by declaring it `nonlocal`.

Deephaven also doesn't search Python's built-in namespace. To use a Python built-in such as `sorted` in a [query string](./query-string-overview.md), assign it to a variable in the local or global scope.

Some [query strings](./query-string-overview.md) can't use a function's local variables at all. They see only the global scope of the main script. The following query strings work this way:

- [Formulas](./formulas.md) in [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) operations, such as [`rolling_formula_tick`](../reference/table-operations/update-by-operations/rolling-formula-tick.md).
- [Filters](./filters.md) passed to a [partitioned table](./partitioned-tables.md)'s [`filter`](../reference/table-operations/partitioned-tables/partitioned-table-filter.md) method.
- [Filters](./filters.md) passed to the [`with_filters`](../reference/table-operations/create/rollup.md#methods) method of a [rollup table](./rollup-table.md), and [formulas](./formulas.md) passed to its [`with_update_view`](../reference/table-operations/create/rollup.md#methods) method.
- [Filters](./filters.md) passed to the [`with_filters`](/core/pydoc/code/deephaven.table.html#deephaven.table.TreeTable.with_filters) method of a [tree table](./tree-table.md).
- [Filters](./filters.md) and [formulas](./formulas.md) in node operations. You record these with the recorder that [`node_operation_recorder`](../reference/table-operations/create/rollup.md#methods) returns, then apply them with [`with_node_operations`](../reference/table-operations/create/rollup.md#methods).

Deephaven's own [built-in functions, constants, and variables](#built-in-functions-constants-and-variables) are always available and don't come from the query scope.

## Examples

### Local (function) scope

Variables defined in the local scope of a function or lambda expression are usable only from within that function or lambda. In the following example, `use_local_and_global_vars` builds a table from a local variable and a global variable, then returns it to the outermost scope as `result`.

```python order=result
from deephaven import empty_table

global_var = 99


def use_local_and_global_vars(num_rows):
    local_var = 23
    return empty_table(num_rows).update(["X = global_var * i", "Y = local_var * i"])


result = use_local_and_global_vars(10)
```

The local scope is the scope of the function that calls the table operation itself. If a helper function calls [`update`](../reference/table-operations/select/update.md) for you, the [query string](./query-string-overview.md) sees the helper's local variables, not the caller's.

### Global (module) scope

Variables and functions defined at the outermost scope of a script are usable from any [query string](./query-string-overview.md) in that script unless a local variable with the same name takes precedence. The following query creates two variables and a function in the outermost scope, then creates tables that use them.

```python order=result1,result2,result3
from deephaven import empty_table

a = 1
b = 2


def my_func() -> int:
    return 5


result1 = empty_table(1).update(["A = a", "B = b"])
result2 = empty_table(5).update(["A = i * a", "B = i * b"])
result3 = empty_table(1).update(["X = my_func()"])
```

### Enclosing (nonlocal) scope

The enclosing scope is a special case. [Query strings](./query-string-overview.md) do not see variables in an enclosing function's scope unless the inner function's own code refers to them. The following example declares the variable `nonlocal` so that the inner function refers to it, then uses it to [update](../reference/table-operations/select/update.md) a table.

```python order=result
from deephaven import empty_table


def outer_func():
    enclosing_var = 2

    def inner_func():
        nonlocal enclosing_var
        local_var = 7
        return empty_table(5).update(["X = i * local_var", "Y = i * enclosing_var"])

    return inner_func()


result = outer_func()
```

### Columns take precedence over variables

A column with the same name as a variable wins. In the following example, `a` in the [formula](./formulas.md) `B = a + 1` refers to the column `a`, not the variable `a`, so `B` is 1, 3, and 5 rather than 101.

```python order=result
from deephaven import empty_table

a = 100

result = empty_table(3).update(["a = i * 2", "B = a + 1"])
```

### Encapsulated query logic in functions

Wrapping query logic in a function is a common way to keep code clean and readable. Building on [Local (function) scope](#local-function-scope), you can wrap a whole query in a function and pass the values that change as parameters. Function parameters are in the function's local scope, so [query strings](./query-string-overview.md) inside the function can use them directly.

In the following example, we want to know how much sales tax we'll pay in several states for a given purchase amount. The [query string](./query-string-overview.md) in `compute_tax` uses the function's `sales_price` parameter, so calling `compute_tax` with a different amount changes the result without changing the query.

```python order=source,result1,result2
from deephaven import new_table
from deephaven.column import string_col, double_col


def compute_tax(source, sales_price):
    return source.update(formulas=["SalesTax = SalesTaxRate * sales_price * 0.01"])


source = new_table(
    [
        string_col("State", ["CO", "WY", "UT", "AZ", "NV"]),
        double_col("SalesTaxRate", [2.9, 4, 5.95, 5.6, 6.85]),
    ]
)

result1 = compute_tax(source, 500)
result2 = compute_tax(source, 300)
```

## Built-in functions, constants, and variables

Deephaven provides [built-in functions](./built-in-functions.md), [constants](./built-in-constants.md), and [variables](./built-in-variables.md) that [query strings](./query-string-overview.md) can use without defining them in Python. They are not [Python variables](./python-variables.md). The engine imports the functions and constants into the query language and recognizes the variables [`i`](./built-in-variables.md), [`ii`](./built-in-variables.md), and [`k`](./built-in-variables.md) by name.

The following query uses the built-in function [`sin`](/core/javadoc/io/deephaven/function/Numeric.html#sin(long)), the built-in constant [`MINUTE`](./built-in-constants.md), and the built-in variables [`i`](./built-in-variables.md) and [`ii`](./built-in-variables.md). Some [refreshing tables](../conceptual/table-types.md) restrict the built-in variables. For details, see [Built-in query language variables](./built-in-variables.md#refreshing-table-restrictions).

```python order=result
from deephaven import empty_table

result = empty_table(5).update(["X = sin(ii)", "Y = MINUTE * i"])
```

## Related documentation

- [Deephaven's table types](../conceptual/table-types.md)
- [Arrays](./work-with-arrays.md)
- [Query language built-in constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Create static tables](./new-and-empty-table.md#new_table)
- [An overview of query strings](./query-string-overview.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [Formulas](./formulas.md)
- [Filters](./filters.md)
- [Create and use partitioned tables](./partitioned-tables.md)
- [Create a hierarchical rollup table programmatically](./rollup-table.md)
- [Create a hierarchical tree table](./tree-table.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`double_col`](../reference/table-operations/create/doubleCol.md)
- [`string_col`](../reference/table-operations/create/stringCol.md)
- [`update`](../reference/table-operations/select/update.md)
- [`update_by`](../reference/table-operations/update-by-operations/updateBy.md)
- [`rolling_formula_tick`](../reference/table-operations/update-by-operations/rolling-formula-tick.md)
- [`filter`](../reference/table-operations/partitioned-tables/partitioned-table-filter.md)
- [`rollup`](../reference/table-operations/create/rollup.md)
- [`tree`](../reference/table-operations/create/tree.md)
- [`deephaven.column` Pydoc](/core/pydoc/code/deephaven.column.html)
