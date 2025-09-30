---
title: Variables and functions in query strings
---

This guide will show you techniques for using variables and functions in query strings. There are many reasons to use variables: more understandable code, better reusability, and in some cases, improved efficiency.

In Groovy, the Deephaven Query Language resolves variables using the [`QueryScope`](/core/javadoc/io/deephaven/engine/context/QueryScope.html) singleton. Unlike some other languages, Groovy requires explicit management of the query scope for most use cases.

This guide will walk you through Groovy's scoping rules and the process for adding variables and using functions in Groovy query strings.

If you'd like to learn more about query strings and the basic rationale of the query scope, see our [conceptual guide](../how-to-guides/queryscope.md).

> [!NOTE]
> Variable names and function names are case-sensitive.

## Query scope in Groovy

When the Deephaven engine compiles a query string, it must be able to resolve all variables in the query string using the query scope. Groovy's query scope resolution follows these rules in order:

1. **Local (function) scope**: Variables explicitly added to the query scope using `QueryScope.addParam()` are checked first
2. **Script-level scope**: Variables defined at the top level of a Groovy script are automatically added to the query scope and checked second
3. **Global scope**: Built-in functions and globally available variables are checked last

This means that local variables (when explicitly added) take precedence over script-level variables, which in turn take precedence over global variables. Unlike Python's LEGB (Local, Enclosing, Global, Built-in) scoping rule where variables are automatically resolved, Groovy requires explicit management of the query scope for variables that are not at the script level.

### Script-level scope

Variables defined at the top level of a Groovy script are automatically available in query strings without any additional setup.

```groovy order=source,result
globalVar = 42

source = newTable(intCol("A", 1, 2, 3))

result = source.update("X = A * globalVar")
```

### Local (function) scope

Variables defined within functions, closures, or any local scope must be explicitly added to the query scope to be used in query strings.

```groovy order=source,result
def processWithLocalVar(Table table, int multiplier) {
    // Local variable must be explicitly added to query scope
    QueryScope.addParam("multiplier", multiplier)
    return table.update("X = A * multiplier")
}

source = newTable(intCol("A", 1, 2, 3))

result = processWithLocalVar(source, 5)
```

### Enclosing scope

Variables from enclosing scopes (such as variables in outer functions) are not automatically available in query strings and must be explicitly added to the query scope.

```groovy order=result
def outerFunction() {
    def enclosingVar = 10
    
    def innerFunction = { Table table ->
        // Enclosing variable must be explicitly added to query scope
        QueryScope.addParam("enclosingVar", enclosingVar)
        return table.update("X = A * enclosingVar")
    }
    
    def source = newTable(intCol("A", 1, 2, 3))
    return innerFunction(source)
}

result = outerFunction()
```

## Use variables in a query string

In Groovy, variables can be made available to query strings in two ways: [manually](#manually) using `QueryScope.addParam()` and [automatically](#automatically) for top-level script variables.

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

Variables defined at the top level of a Groovy script are automatically added to the query scope. This only applies to variables declared directly in the script, not within functions or closures.

In the next example, `b = 3` is defined at the top level, so it's automatically available in query strings.

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

It is very common to encapsulate query logic within functions to create cleaner, more readable code. Such functions may use variables in query strings. In Groovy, function parameters and local variables are **not** automatically available to query strings - they must be manually added to the query scope using `QueryScope.addParam()`.

In this example, the `compute` function performs a query using the `source` table and the input parameter `a`. Since `a` is a function parameter (not a top-level script variable), it must be explicitly added to the query scope to be used in the query string.

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

Note that `sales_price` must be explicitly added to the query scope since it's a function parameter, not a top-level variable.

```groovy order=source,result1,result2
computeTax = { Table source, int a ->
    // Function parameters must be explicitly added to query scope
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
