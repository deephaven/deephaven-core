---
title: Query Scope
---

Deephaven [query strings](./query-string-overview.md) allow complex queries to be expressed with a concise syntax. These query strings can implement [formulas](./formulas.md) or [filters](./filters.md) that use Python variables. For example, the [query string](./query-string-overview.md) in this [`update`](../reference/table-operations/select/update.md) uses the variable `a` from Python.

```python skip-test
a = 2
t2 = t.update(["Y = a * X"])
```

When the Deephaven engine compiles a [query string](./query-string-overview.md), it must be able to resolve all variables in the query string. This is done using the query scope. The Deephaven query scope follows rules similar to Python's [LEGB](https://realpython.com/python-scope-legb-rule/) rule, with some important distinctions.

Query strings in Deephaven Python queries resolve variables with the following precedence:

- Local (function) scope
- Global (module) scope

The enclosing (nonlocal) scope is not directly supported. If you want to use a variable in the enclosing scope in a query string, you _must_ use the `nonlocal` keyword to do so.

## Examples

### Local (function) scope

Variables defined in the local scope of a function or lambda expression are usable only from within that function or lambda. The following query uses the local scope to create a table, which is visible in the outermost scope. The function uses both a local and global variable to create the table.

```python order=result
from deephaven import empty_table

global_var = 99


def use_local_and_global_vars(num_rows):
    local_var = 23
    return empty_table(num_rows).update(["X = global_var * i", "Y = local_var * i"])


result = use_local_and_global_vars(10)
```

### Enclosing (nonlocal) scope

Variables that exist in the enclosing scope are usable from only within the enclosing scope. If you wish to use an object in the enclosing scope in a query string, you must use the `nonlocal` keyword before using it. The following query uses a variable in the enclosing scope to update a table.

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

### Global (module) scope

Variables and functions defined at the outermost scope are usable from within any query string in a query. The following query creates two variables and a function in the outermost scope, then creates tables that use them.

```python order=result1,result2,result3
from deephaven import empty_table

a = 1
b = 2


def my_func() -> int:
    return 5


result1 = empty_table(1).update(["A = a", "B = b"])
result2 = empty_table(5).update(["A = i * a", "B = i * b"])
result3 = empty_table(1).update("X = my_func()")
```

### Encapsulated query logic in functions

One can encapsulate query logic within functions. Such functions may use variables in query strings.

In the following example, the `compute` function performs a query using the `source` table and the input parameter `a`. Here, `a` is defined in the local scope of `compute`, and therefore can only be used from within the function itself.

```python order=source,result1,result2
from deephaven import new_table
from deephaven.column import int_col

import numpy as np


def f(a, b) -> np.intc:
    return a * b


def compute(source, a):
    return source.update(formulas=["X = f(a, A)"])


source = new_table([int_col("A", [1, 2, 3])])

result1 = compute(source, 10)
result2 = compute(source, 3)
```

## Related documentation

- [Built-in query language constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Create a new table](./new-and-empty-table.md#new_table)
- [How to use variables and functions in query strings](./query-scope.md)
- [Use variables in query strings](./python-variables.md)
- [Use functions in query strings](./python-functions.md)
- [Query language formulas](./formulas.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
