---
id: work-with-strings
title: Strings
---

A [String](https://en.wikipedia.org/wiki/String_(computer_science)) is a sequence of characters, such as letters, numbers, or symbols, that is treated as a single data type. Strings frequently contain words or text. Deephaven queries use strings ubiquitously. The Deephaven engine uses [`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html).

## Java strings in queries

String columns in Deephaven tables are stored as [`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html) objects. This provides three powerful ways to manipulate text in [query strings](./query-string-overview.md):

1. Native Java string methods (like `length`, `substring`, etc.)
2. Deephaven's [built-in string functions](./built-in-functions.md)
3. String-specific [operators](./operators.md) (such as `+` for concatenation)

### String concatenation

[`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html) objects can be concatenated with the `+` [operator](./operators.md):

```groovy order=source,sourceMeta
source = emptyTable(1).update("StrCol1 = `Hello, `", "StrCol2 = `Deephaven!`", "StrCol3 = StrCol1 + StrCol2")
sourceMeta = source.meta()
```

### String methods

[`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html) methods can be called from [query strings](./query-string-overview.md) and can yield very powerful queries:

```groovy order=source,result
source = emptyTable(1).update(
    "StrCol1 = `Learning `",
    "StrCol2 = `about `",
    "StrCol3 = ` strings.`",
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

### String filtering

A common use of string methods is for [string filtering](./filters.md#string-filters):

```groovy order=source,result
source = newTable(
    stringCol("X", "Aa", "Ba", "Bb", "Ca", "Bc", "Ab", "Bd", "Be", "Cb")
)

result = source.where("X.startsWith(`C`)")
```

### Built-in functions

Many [built-in functions](./built-in-functions.md) work with strings. For instance, you can parse the numeric values in strings:

```groovy order=result,resultMeta,source
source = emptyTable(1).update(
    "StringInt = `4`",
    "StringFloat = `4.5`",
    "StringDouble = `3.14159`",
    "StringBool = `true`",
)
result = source.update(
    "Int = parseInt(StringInt)",
    "Float = parseFloat(StringFloat)",
    "Double = parseDouble(StringDouble)",
    "Bool = parseBoolean(StringBool)",
)
resultMeta = result.meta()
```

## Related documentation

- [Filter table data](./filters.md)
- [New and empty table](./new-and-empty-table.md#newtable)
- [Query strings](./query-string-overview.md)
- [String literals in query strings](./string-char-literals.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Groovy closures in query strings](./groovy-closures.md)
- [Java objects in query strings](./java-classes.md)
- [Filters](./filters.md)
- [Formulas](./formulas.md)
- [Operators in query strings](./operators.md)
