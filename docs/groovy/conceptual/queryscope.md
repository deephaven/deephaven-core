---
title: Use variables in query strings
sidebar_label: Variables in query strings
---

This guide will explain techniques for using variables and functions in query strings. There are many reasons to use variables: more understandable code, better reusability, and in some cases, improved efficiency.

We'll start with an example query, and then unpack each element as we go along to home in on specific concepts.

Consider the following common query.

```groovy order=source,result
var = 3

f = { a, b ->
    return a + b
}

source = newTable(
    intCol("A", 1, 2, 3, 4, 5),
    intCol("B", 10, 20, 30, 40, 50)

)

result = source.update("X = A + 3 * sqrt(B) + var + (int) f(A, B)")
```

In this example, the query string is the combined expression `X = A + 3 * sqrt(B) + var + (int) f(A, B)`. The entire expresion of `X = A + 3 * sqrt(B) + var + (int) f(A, B)` above is a query string. It is passed to a query and executed against data - it is a fully formed statement. Creating a query string involves using columns, variables, operators, keywords, expressions, and methods to compute the desired result.

Inside the query string are several elements:

- `X` is a column in the new table.
- `A` and `B` are columns in the source table.
- `*` is an operator.
- `sqrt` is the built-in square root function.
- `(int)` is a cast to ensure the function `f()` returns an integer and not a string.
- `f()` is our previously defined function.
- `var` is a variable.

A compiler inside Deephaven converts the query string into executable code. As part of the compilation, all of the symbols must be associated with values. The [query scope](../reference/query-language/variables/scope.md) is used to resolve the variable values.

In Python, the Deephaven Query Language resolves variables using the current global symbol table, which can be accessed using [`globals()`](https://docs.python.org/3/library/functions.html#globals). In Groovy, the Deephaven Query Language resolves variables using the [`QueryScope`](/core/javadoc/io/deephaven/engine/context/QueryScope.html) singleton.

For more information, see [How to use variables and functions in query strings](../how-to-guides/queryscope.md) or the [query scope](../reference/query-language/variables/scope.md) reference documentation.

## Related documentation

- [Create a new table](../how-to-guides/new-and-empty-table.md#newtable)
- [How to use variables and functions in query strings](../how-to-guides/queryscope.md)
- [Query scope](../reference/query-language/variables/scope.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/context/QueryScope.html)
