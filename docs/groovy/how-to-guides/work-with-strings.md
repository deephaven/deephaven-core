---
title: Strings
---

A [string](https://en.wikipedia.org/wiki/String_(computer_science)) is a sequence of characters, such as letters, numbers, or symbols, that a program handles as a single value. Deephaven queries use strings for column values, [filters](./filters.md), and [formulas](./formulas.md).

The Deephaven engine represents strings as [`java.lang.String`](../reference/query-language/types/strings.md). This guide shows how to use Groovy strings and Java strings in [query strings](./query-string-overview.md).

## Groovy strings in queries

You can use [Groovy string variables](./groovy-variables.md#strings) in [query strings](./query-string-overview.md) directly. A Groovy string literal such as `"Deephaven!"` is already a [`java.lang.String`](../reference/query-language/types/strings.md), so the resulting column is a [`java.lang.String`](../reference/query-language/types/strings.md) column.

Inside a [query string](./query-string-overview.md), a [string literal](./string-char-literals.md#string-literals) usually goes between backticks, which avoids escaping double quotes.

Let's add the literal `` `Hello, ` `` to the variable `myStr`. The `sourceMeta` table, from [`meta`](../reference/table-operations/metadata/meta.md), lists each column's name and type. Its `DataType` column shows `java.lang.String`:

```groovy order=source,sourceMeta
myStr = "Deephaven!"

source = emptyTable(1).update("MyStringColumn = `Hello, ` + myStr")
sourceMeta = source.meta()
```

An interpolated string such as `"Hello ${name}"` is a Groovy `GString`, not a [`java.lang.String`](../reference/query-language/types/strings.md). In a [query string](./query-string-overview.md), it makes a `GStringImpl` column. A [match filter](./filters.md#match-filters) such as `"S = myStr"` on a [`String`](../reference/query-language/types/strings.md) column then finds no rows and raises no error. To get a [`String`](../reference/query-language/types/strings.md), call `toString` on it when you assign it, for example `myStr = "Hello ${name}".toString()`.

You can also call [Groovy closures](./groovy-closures.md) that return strings from [query strings](./query-string-overview.md). The engine sees every closure call as returning `Object`, so [cast](./casting.md#groovy-closures) the result to [`String`](../reference/query-language/types/strings.md) to get a [`String`](../reference/query-language/types/strings.md) column:

```groovy order=source,sourceMeta
stringClosure = { "Hello, Deephaven!" }

source = emptyTable(1).update("StringColumn = (String)stringClosure()")
sourceMeta = source.meta()
```

## Java strings in queries

Deephaven stores string columns as [`java.lang.String`](../reference/query-language/types/strings.md) objects, so you work with them in [query strings](./query-string-overview.md) the way Java does:

- String concatenation with the `+` [operator](./operators.md), which concatenates when either operand is a string
- Native [`java.lang.String`](../reference/query-language/types/strings.md) methods, such as [`indexOf`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html#indexOf(java.lang.String)) and [`substring`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html#substring(int,int))
- Deephaven's [built-in functions](./built-in-functions.md) that work with strings

### String concatenation

The `+` [operator](./operators.md) concatenates [`java.lang.String`](../reference/query-language/types/strings.md) objects:

```groovy order=source,sourceMeta
source = emptyTable(1).update(
    "StrCol1 = `Hello, `",
    "StrCol2 = `Deephaven!`",
    "StrCol3 = StrCol1 + StrCol2"
)
sourceMeta = source.meta()
```

### String methods

You can call [`java.lang.String`](../reference/query-language/types/strings.md) methods directly from [query strings](./query-string-overview.md) to search, slice, and change the case of text without leaving the query:

```groovy order=source,result
source = emptyTable(1).update(
    "StrCol1 = `Learning `",
    "StrCol2 = `about `",
    "StrCol3 = `strings.`",
    "StrCol4 = StrCol1 + StrCol2 + StrCol3"
)
result = source.update(
    "IndexOfSubstring = StrCol4.indexOf(`ings.`)",
    "StringIsEmpty = StrCol2.isEmpty()",
    "Substring = StrCol4.substring(4, 14)",
    "UpperCase = StrCol1.toUpperCase()",
    "LowerCase = StrCol3.toLowerCase()"
)
```

In `Learning about strings.`, the text `ings.` starts at index 18, so `IndexOfSubstring` is `18`. `Substring` is `ning about`, the characters from index 4 up to but not including index 14.

#### String filtering

A common use of string methods is [string filtering](./use-filters.md#string-filters):

```groovy order=source,result
source = newTable(
    stringCol("X", "Aa", "Ba", "Bb", "Ca", "Bc", "Ab", "Bd", "Be", "Cb")
)

result = source.where("X.startsWith(`C`)")
```

`result` keeps only the rows whose `X` value starts with `C`: `Ca` and `Cb`.

### Built-in functions

Many [built-in functions](./built-in-functions.md) work with strings. For instance, the [parse functions](../reference/query-language/query-library/auto-imported/parse.md) convert strings to numeric and boolean values:

```groovy order=source,result,resultMeta
source = emptyTable(1).update(
    "StringInt = `4`",
    "StringFloat = `4.5`",
    "StringDouble = `3.14159`",
    "StringBool = `true`"
)
result = source.update(
    "Int = parseInt(StringInt)",
    "Float = parseFloat(StringFloat)",
    "Double = parseDouble(StringDouble)",
    "Bool = parseBoolean(StringBool)"
)
resultMeta = result.meta()
```

`resultMeta` shows that `Int` is an `int` column, `Float` a `float` column, `Double` a `double` column, and `Bool` a `java.lang.Boolean` column.

## Related documentation

- [Filter table data](./use-filters.md)
- [Create static tables](./new-and-empty-table.md)
- [An overview of query strings](./query-string-overview.md)
- [String and char literals in query strings](./string-char-literals.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Groovy closures in queries](./groovy-closures.md)
- [Casting](./casting.md)
- [Java objects and classes in query strings](./java-classes.md)
- [Built-in query language functions](./built-in-functions.md)
- [Filters](./filters.md)
- [Formulas](./formulas.md)
- [Operators](./operators.md)
- [Strings](../reference/query-language/types/strings.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
- [`stringCol`](../reference/table-operations/create/stringCol.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`meta`](../reference/table-operations/metadata/meta.md)
- [Parse functions](../reference/query-language/query-library/auto-imported/parse.md)
- [`emptyTable` Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#emptyTable(long))
- [`newTable` Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#newTable(io.deephaven.engine.table.impl.util.ColumnHolder...))
- [`stringCol` Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#stringCol(java.lang.String,java.lang.String...))
- [`update` Javadoc](/core/javadoc/io/deephaven/api/TableOperations.html#update(java.lang.String...))
- [`where` Javadoc](/core/javadoc/io/deephaven/api/TableOperations.html#where(java.lang.String...))
- [`meta` Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html#meta())
