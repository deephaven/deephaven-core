---
title: Query scope
---

The query scope allows users to define variables and functions in query strings.

In Python, the Deephaven Query Language resolves variables using the current global symbol table. In Groovy, the Deephaven query language resolves variables using the [`QueryScope`](/core/javadoc/io/deephaven/engine/context/QueryScope.html) singleton.

## Syntax

```groovy skip-test
QueryScope.addParam(name, value)
```

## Examples

> [!NOTE]
> Variable names and function names are case-sensitive.

### Manually add variables

In the following example, the variable `a` is manually added and assigned the value 7.

```groovy
QueryScope.addParam("a", 7)

result = emptyTable(5).update("Y = a")
```

This variable can be used inside a query string.

```groovy order=source,result
QueryScope.addParam("a", 7)

source = newTable(intCol("A", 1, 2, 3))

result = source.update("X = 2 + 3 * sqrt(A) + a")
```

### Automatically add variables

At the top level of a program, variables are automatically added to the query scope.

In the following example, a program contains `b = 3` at the top level, so the query scope will also contain the name `b` with a value of 3.

```groovy
b = 3

result = emptyTable(5).update("Y = b")
```

This variable can be used inside a query string.

```groovy order=source,result
b = 3

source = newTable(intCol("A", 1, 2, 3))

result = source.update("X = 2 + 3 * sqrt(A) + b")
```

### User-defined functions in a query string

Programming languages frequently implement functions as callable [objects](../types/objects.md). The query scope can make these callable [objects](../types/objects.md) available for use inside query strings.

In the following example, `myFunction` is defined and is called from the query string.

```groovy order=source,result
myFunction = { int a -> a * (a + 1) }

source = newTable(intCol("A", 1, 2, 3))

result = source.update("X = 2 + 3 * (int)myFunction(A)")
```

### Encapsulated query logic in functions

One can encapsulate query logic within functions. Such functions may use variables in query strings. In these cases, the variables need to be manually added to the query scope.

In the following example, the `compute` function performs a query using the `source` table and the input parameter `a`. Here, `a` must be added to the query scope to be used in the query string.

```groovy order=source,result1,result2
f = { int a, int b -> a * b }

compute = { Table source, int a ->

    // Values must be added to the query scope to be used in query strings.
    QueryScope.addParam("a", a)

    return source.update("X = (int)f(a, A)")
}

source = newTable(intCol("A", 1, 2, 3))

result1 = compute(source, 10)
result2 = compute(source, 3)
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to use variables and functions in query strings](../../../how-to-guides/queryscope.md)
- [Use variables in query strings](../../../how-to-guides/queryscope.md)
- [How to use Deephaven's built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [Query language functions](../query-library/query-language-function-reference.md)
- [`emptyTable`](../../table-operations/create/emptyTable.md)
- [`newTable`](../../table-operations/create/newTable.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/context/QueryScope.html)
