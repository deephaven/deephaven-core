---
title: Query scope
---

Deephaven [query strings](./query-string-overview.md) let you express complex queries in a concise syntax. These [query strings](./query-string-overview.md) can implement [formulas](./formulas.md) or [filters](./filters.md) that use [Groovy variables](./groovy-variables.md). For example, the [query string](./query-string-overview.md) in this [`update`](../reference/table-operations/select/update.md) uses the variable `a` from Groovy. Variables make queries easier to understand and reuse.

```groovy skip-test
a = 2
t2 = t.update("Y = a * X")
```

The query scope is the set of user-defined variables and functions that [query strings](./query-string-overview.md) can refer to. When Deephaven evaluates `Y = a * X`, it looks up `a` in the query scope. This guide walks you through how Deephaven resolves [Groovy variables](./groovy-variables.md) and functions in [query strings](./query-string-overview.md). To learn how to use each kind of variable and function, see [Groovy variables in query strings](./groovy-variables.md) and [Groovy closures in queries](./groovy-closures.md).

> [!NOTE]
> Variable names and function names are case-sensitive.

## Query scope in Groovy

In Groovy, [query strings](./query-string-overview.md) resolve variables through the [`QueryScope`](/core/javadoc/io/deephaven/engine/context/QueryScope.html). In a Groovy session, the query scope is the script session's variable binding: the set of named variables the session holds. The following rules decide which variables the binding contains:

- Variables assigned at the top level of a Groovy script without `def` or a type (for example, `a = 2`) are in the query scope without a call to [`addParam`](/core/javadoc/io/deephaven/engine/context/QueryScope.html#addParam(java.lang.String,T)).
- Variables declared with `def` or a type, even at the top level (for example, `def a = 2` or `int a = 2`), and variables local to a method or [closure](./groovy-closures.md) are not in the query scope. Add them with [`QueryScope.addParam(name, value)`](/core/javadoc/io/deephaven/engine/context/QueryScope.html#addParam(java.lang.String,T)).
- [`addParam`](/core/javadoc/io/deephaven/engine/context/QueryScope.html#addParam(java.lang.String,T)) writes into the same binding as top-level variables. It overwrites any existing variable with that name, and the variable stays available after the method or [closure](./groovy-closures.md) returns.

When a [query string](./query-string-overview.md) uses a name, the engine resolves it in this order:

1. A column, a [column array](./work-with-arrays.md#using-the-underscore-operator) such as `A_`, or a [built-in variable](./built-in-variables.md) ([`i`](./built-in-variables.md), [`ii`](./built-in-variables.md), [`k`](./built-in-variables.md)).
2. A variable in the binding.
3. A [built-in constant](./built-in-constants.md). A variable named `MINUTE` therefore hides the built-in [`MINUTE`](./built-in-constants.md).

A variable name that isn't found in any of these places fails with a `Cannot find variable or class` error.

Function calls resolve the other way around: a [built-in function](./built-in-functions.md) wins over a [closure](./groovy-closures.md) with the same name when one of its overloads accepts the arguments.

Deephaven's own [built-in functions, constants, and variables](#built-in-functions-constants-and-variables) are always available and don't come from the query scope.

## Examples

### Script-level scope

In the following example, `globalVar` is assigned at the top level without `def`, so the [query string](./query-string-overview.md) can use it directly.

```groovy order=source,result
globalVar = 42

source = newTable(intCol("A", 1, 2, 3))

result = source.update("X = A * globalVar")
```

A top-level variable declared with `def` is not in the binding, so a [query string](./query-string-overview.md) can't use it. The following query fails with a `Cannot find variable or class factor` error:

```groovy should-fail
def factor = 3

result = emptyTable(3).update("X = i * factor")
```

Assign the variable without `def` instead:

```groovy order=result
factor = 3

result = emptyTable(3).update("X = i * factor")
```

### Script-level functions

A [closure](./groovy-closures.md) assigned to a top-level variable is in the binding, so a [query string](./query-string-overview.md) can call it. A script method, such as `def myFunction(int a) { ... }`, is not in the binding, and a [query string](./query-string-overview.md) can't call it. Assign a [closure](./groovy-closures.md) to a variable instead.

In the following example, the [query string](./query-string-overview.md) calls the top-level [closure](./groovy-closures.md) `myFunction`.

```groovy order=source,result
myFunction = { int a -> a * (a + 1) }

source = newTable(intCol("A", 1, 2, 3))

result = source.update("X = 2 + 3 * (int)myFunction(A)")
```

The `(int)` cast is needed because a [closure](./groovy-closures.md) called from a [query string](./query-string-overview.md) returns [`Object`](../reference/query-language/types/objects.md). For details, see [Groovy closures in queries](./groovy-closures.md).

### Local (function) scope

To use a variable defined in a method, [closure](./groovy-closures.md), or any other local scope in a [query string](./query-string-overview.md), you must add it to the query scope with [`QueryScope.addParam`](/core/javadoc/io/deephaven/engine/context/QueryScope.html#addParam(java.lang.String,T)):

<a id="manually"></a>

```groovy syntax
QueryScope.addParam(name, value)
```

In the following example, `processWithLocalVar` defines a local variable, `multiplier`, and adds it to the query scope with [`QueryScope.addParam`](/core/javadoc/io/deephaven/engine/context/QueryScope.html#addParam(java.lang.String,T)) before the [query string](./query-string-overview.md) uses it.

```groovy order=source,result
def processWithLocalVar(Table table) {
    def multiplier = 5
    // Local variable must be explicitly added to query scope
    QueryScope.addParam("multiplier", multiplier)
    return table.update("X = A * multiplier")
}

source = newTable(intCol("A", 1, 2, 3))

result = processWithLocalVar(source)
```

A variable from an enclosing method or [closure](./groovy-closures.md) needs the same call. A [closure](./groovy-closures.md) can capture it, but the [query string](./query-string-overview.md) can't see it until you add it with [`QueryScope.addParam`](/core/javadoc/io/deephaven/engine/context/QueryScope.html#addParam(java.lang.String,T)).

### Columns take precedence over variables

A column with the same name as a variable wins. In the following example, `a` in the [formula](./formulas.md) `B = a + 1` refers to the column `a`, not the variable `a`, so `B` is 1, 3, and 5 rather than 101.

```groovy order=result
a = 100

result = emptyTable(3).update("a = i * 2", "B = a + 1")
```

### Encapsulated query logic in closures

Wrapping query logic in a [closure](./groovy-closures.md) is a common way to keep code clean and readable. Building on [Local (function) scope](#local-function-scope), you can wrap a whole query in a [closure](./groovy-closures.md) and pass the values that change as parameters. Add each parameter to the query scope before the [query string](./query-string-overview.md) uses it.

In the following example, we want to know how much sales tax we'll pay in several states for a given purchase amount. The `computeTax` [closure](./groovy-closures.md) adds its `salesPrice` parameter to the query scope, because a [closure](./groovy-closures.md) parameter is not a top-level variable. Calling `computeTax` with a different amount changes the result without changing the query.

```groovy order=source,result1,result2
computeTax = { Table source, int salesPrice ->
    // Closure parameters must be explicitly added to the query scope
    QueryScope.addParam("salesPrice", salesPrice)
    return source.update("SalesTax = SalesTaxRate * salesPrice * 0.01")
}

source = newTable(
    stringCol("State", "CO", "WY", "UT", "AZ", "NV"),
    doubleCol("SalesTaxRate", 2.9, 4, 5.95, 5.6, 6.85)
)

result1 = computeTax(source, 500)
result2 = computeTax(source, 300)
```

## Built-in functions, constants, and variables

Deephaven provides [built-in functions](./built-in-functions.md), [constants](./built-in-constants.md), and [variables](./built-in-variables.md) that [query strings](./query-string-overview.md) can use without adding them to the query scope. The engine imports the functions and constants into the query language and recognizes the variables [`i`](./built-in-variables.md), [`ii`](./built-in-variables.md), and [`k`](./built-in-variables.md) by name.

The following query uses the built-in function [`sin`](/core/javadoc/io/deephaven/function/Numeric.html#sin(long)), the built-in constant [`MINUTE`](./built-in-constants.md), and the built-in variables [`i`](./built-in-variables.md) and [`ii`](./built-in-variables.md). Some [refreshing tables](../conceptual/table-types.md) restrict the built-in variables. For details, see [Built-in query language variables](./built-in-variables.md#refreshing-table-restrictions).

```groovy order=result
result = emptyTable(5).update("X = sin(ii)", "Y = MINUTE * i")
```

## Related documentation

- [Deephaven's table types](../conceptual/table-types.md)
- [Arrays](./work-with-arrays.md)
- [Query language built-in constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Create static tables](./new-and-empty-table.md#newtable)
- [An overview of query strings](./query-string-overview.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Groovy closures in queries](./groovy-closures.md)
- [Formulas](./formulas.md)
- [Filters](./filters.md)
- [Query scope reference](../reference/query-language/variables/scope.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`doubleCol`](../reference/table-operations/create/doubleCol.md)
- [`intCol`](../reference/table-operations/create/intCol.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
- [`stringCol`](../reference/table-operations/create/stringCol.md)
- [`update`](../reference/table-operations/select/update.md)
- [`QueryScope` Javadoc](/core/javadoc/io/deephaven/engine/context/QueryScope.html)
