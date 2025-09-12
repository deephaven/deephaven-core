---
title: Variables and functions in query strings
---

This guide will show you techniques for using variables and functions in query strings. There are many reasons to use variables: more understandable code, better reusability, and in some cases, improved efficiency.

The Deephaven Query Language resolves variables using the [`QueryScope`](/core/javadoc/io/deephaven/engine/context/QueryScope.html) singleton.

This example case will walk you through the process for adding variables and using functions in your preferred programming language.

If you'd like to learn more about query strings and the basic rationale of the query scope, see our [conceptual guide](../how-to-guides/queryscope.md).

> [!NOTE]
> Variable names and function names are case-sensitive.

## Use variables in a query string

There are two ways to add variables to a query string: [manually](#manually) and [automatically](#automatically).

### Manually

To add the variable by hand, use this basic syntax:

```groovy skip-test
QueryScope.addParam(name, value)
```

In the following example, the value 7 is assigned to the variable `a`.

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

### Automatically

If you are at the top level of a program, variables are automatically added to the query scope.

In the next example, a program contains `b = 3` at the top level, so the query scope will also contain the name `b` with a value of 3.

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

## Use functions in a query string

We can also define a function and use it inside a query string.

In the next example, `myFunction` is defined and is called from the query string.

```groovy order=source,result
myFunction = { int a -> a * (a + 1) }

source = newTable(intCol("A", 1, 2, 3))

result = source.update("X = 2 + 3 * (int)myFunction(A)")
```

## Encapsulate query logic in functions

It is very common to encapsulate query logic within functions to create cleaner, more readable code. Such functions may use variables in query strings. In these cases, the variables need to be manually added to the query scope.

In this example, the `compute` function performs a query using the `source` table and the input parameter `a`. Here, `a` must be added to the query scope to be used in the query string.

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

## Example

In this example, we want to know how much sales tax we will pay in various states given how much money is spent. The query scope is helpful because we can easily change the value of money spent as we call the function more than once.

```groovy order=source,result1,result2
computeTax = { Table source, int a ->
    QueryScope.addParam("sales_price", a)
    return source.update("Taxed = SalesTaxRate * sales_price * 0.01")
}

source = newTable(
            stringCol("State", "CO", "WY", "UT", "AZ", "NV"),
            doubleCol("SalesTaxRate", 2.9, 4, 5.95, 5.6, 6.85)
        )

result1 = computeTax(source, 500)
result2 = computeTax(source, 300)
```

## Related documentation

- [Understanding the query scope](../how-to-guides/queryscope.md)
- [Create a new table](./new-and-empty-table.md#newtable)
- [Javadoc](/core/javadoc/io/deephaven/engine/context/QueryScope.html)
