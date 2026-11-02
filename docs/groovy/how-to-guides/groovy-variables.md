---
id: groovy-variables
title: Groovy variables in query strings
---

Using your own Groovy variables, [closures](./groovy-closures.md), and [classes](./groovy-classes.md) in Deephaven [query strings](./query-string-overview.md) is one of Deephaven's most powerful features. This guide covers three common kinds of Groovy variables: scalars, strings, and lists or arrays. It starts with the rules that decide which variables a query string can see and what column types they produce.

## Rules for Groovy variables in query strings

[Query strings](./query-string-overview.md) find Groovy variables through Deephaven's [query scope](./query-scope.md). In a Groovy session, the query scope is the script session's variable binding, which holds the variables a script assigns without `def` or a type.

The first four rules below decide which variables a [query string](./query-string-overview.md) can see:

- A variable assigned without `def` or a type is in the binding, so a [query string](./query-string-overview.md) can use it by name. This includes a top-level assignment such as `a = 4`. It also includes an assignment inside a [closure](./groovy-closures.md), such as `{ -> z = 5 }`, once the [closure](./groovy-closures.md) runs.
- A variable declared with `def` or a type, such as `def a = 4` or `int a = 4`, is local to the script and isn't in the binding. Neither is a variable declared inside a method or [closure](./groovy-closures.md). A [query string](./query-string-overview.md) that uses one fails with a "Cannot find variable or class" error. To fix it, add the variable with [`QueryScope.addParam`](/core/javadoc/io/deephaven/engine/context/QueryScope.html#addParam(java.lang.String,T)), as [Local (function) scope](./query-scope.md#local-function-scope) shows.
- A variable with the same name as a column of the table is hidden, even when it's in the binding. The [query string](./query-string-overview.md) reads the column instead, with no error. See [Columns take precedence over variables](./query-scope.md#columns-take-precedence-over-variables).
- A variable named [`i`](../reference/query-language/variables/special-variables.md), [`ii`](../reference/query-language/variables/special-variables.md), or [`k`](../reference/query-language/variables/special-variables.md) is hidden too. Those names are [built-in variables](./built-in-variables.md) for row position and row key, so the [query string](./query-string-overview.md) always reads the built-in. The name `not` is reserved as well. Give your variable a different name.

The last two rules decide what column types those variables produce:

- A column's type comes from the variable's Groovy type:
  - A boxed number becomes a [primitive](./data-types.md#primitive-types). An `Integer` gives an `int` column, and a `Double` gives a `double` column.
  - A `Boolean` gives a `java.lang.Boolean` column.
  - A decimal literal such as `3.14` is a `java.math.BigDecimal` in Groovy, not a `double`. It gives a `BigDecimal` column unless you [cast](./casting.md#groovy-variables) it. See [Scalars](#scalars).
- Indexing into a Groovy list returns [`Object`](../reference/query-language/types/objects.md), so [cast](./casting.md#type-casts) the element to the type you want. Indexing into a Java primitive [array](./work-with-arrays.md), such as `int[]`, keeps the element type. See [Lists and arrays](#lists-and-arrays).

Every example on this page assigns its variables at the top level, without `def` or a type, so they are all in the binding.

## Scalars

Scalars are single values, such as numbers or booleans. A [query string](./query-string-overview.md) uses a scalar by its variable name, with no special syntax. The following example uses Groovy variables in [query strings](./query-string-overview.md) to create new columns in a table:

```groovy order=source,sourceMeta
a = 4
b = 3.14
c = -1.91e7
d = true

source = emptyTable(1).update("A = a", "B = b", "C = c + 1", "D = d")
sourceMeta = source.meta()
```

The [`meta`](../reference/table-operations/metadata/meta.md) table, `sourceMeta`, shows each column's type:

- `A` is an `int`.
- `B` and `C` are `java.math.BigDecimal`. Groovy reads every decimal literal as a `BigDecimal`, including one with an exponent such as `-1.91e7`.
- `D` is a `java.lang.Boolean`.

To get a `double` column instead of a `BigDecimal` one, do one of these:

- Cast the variable in the query string: `"B = (double)b"`. See [type casts](./casting.md#groovy-variables).
- Give the literal a `d` suffix: `b = 3.14d`.

## Strings

Like [scalars](#scalars), a Groovy string goes into a [query string](./query-string-overview.md) by its variable name, with no special syntax. The following example creates a table with two string columns:

```groovy order=source,sourceMeta
myFirstString = "Hello, world!"
mySecondString = "Coding is fun."

source = emptyTable(1).update("FirstString = myFirstString", "SecondString = mySecondString")
sourceMeta = source.meta()
```

The [`meta`](../reference/table-operations/metadata/meta.md) table, `sourceMeta`, shows that both columns are [`java.lang.String`](../reference/query-language/types/strings.md).

A string that interpolates a value, such as `"Hello ${name}"`, is a Groovy `GString`, not a [`java.lang.String`](../reference/query-language/types/strings.md). In a [query string](./query-string-overview.md), it makes a `GStringImpl` column. A [match filter](./filters.md#match-filters) that compares it with a [`String`](../reference/query-language/types/strings.md) column finds no rows, and it raises no error. To avoid this, call `toString` when you assign the string, for example `greeting = "Hello ${name}".toString()`. See [Strings](./work-with-strings.md) for more.

## Lists and arrays

You can also use Groovy lists and Java [arrays](./work-with-arrays.md) in [query strings](./query-string-overview.md). A Groovy list literal such as `[1, 2, 3]` is a `java.util.ArrayList`:

```groovy order=sourceList,sourceListMeta
myList = [1, 2, 3]

sourceList = emptyTable(1).update("ListColumn = myList")
sourceListMeta = sourceList.meta()
```

The `sourceListMeta` table shows that `ListColumn` holds the whole list as a single `java.util.ArrayList` object.

You can also extract individual elements in a [query string](./query-string-overview.md). For a list, `myList[0]` becomes `myList.get(0)`, which returns [`Object`](../reference/query-language/types/objects.md). Use [type casts](./casting.md#type-casts) to get the column type you want:

```groovy order=source,sourceMeta
myList = [1, 2, 3]

source = emptyTable(1).update(
    "FirstElement = (int)myList[0]",
    "SecondElement = (int)myList[1]",
    "ThirdElement = (int)myList[2]",
)
sourceMeta = source.meta()
```

With the [casts](./casting.md#type-casts), `FirstElement`, `SecondElement`, and `ThirdElement` are all `int` columns.

To use a Java primitive [array](./work-with-arrays.md) instead, convert the list with `as`, for example `myArray = [1, 2, 3] as int[]`. Don't declare it as `int[] myArray = [1, 2, 3]`. A typed declaration [isn't in the binding](#rules-for-groovy-variables-in-query-strings), so a [query string](./query-string-overview.md) can't see it.

In the following example, `ArrayColumn` holds the whole `int[]`. Indexing into a primitive [array](./work-with-arrays.md) keeps the element type, so `FirstElement` is an `int` with no cast:

```groovy order=sourceArray,sourceArrayMeta
myArray = [1, 2, 3] as int[]

sourceArray = emptyTable(1).update("ArrayColumn = myArray", "FirstElement = myArray[0]")
sourceArrayMeta = sourceArray.meta()
```

## Related documentation

- [Query scope](./query-scope.md)
- [An overview of query strings](./query-string-overview.md)
- [Groovy closures in queries](./groovy-closures.md)
- [Groovy classes and objects in query strings](./groovy-classes.md)
- [Casting](./casting.md)
- [Built-in query language variables](./built-in-variables.md)
- [Data types in Deephaven](./data-types.md)
- [Arrays](./work-with-arrays.md)
- [Strings](./work-with-strings.md)
- [Filters](./filters.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`meta`](../reference/table-operations/metadata/meta.md)
- [Strings reference](../reference/query-language/types/strings.md)
- [Special variables](../reference/query-language/variables/special-variables.md)
- [`QueryScope` Javadoc](/core/javadoc/io/deephaven/engine/context/QueryScope.html)
