---
title: Work with strings
sidebar_label: Strings
---

[Strings](https://en.wikipedia.org/wiki/String_(computer_science)) are sequences of characters that form words, sentences, phrases, or statements in programming languages. Deephaven queries use strings ubiquitously. Understanding their uses is critical to becoming a strong Deephaven developer.

> [!NOTE]
> This guide assumes you are familiar with using strings in [Java](https://dev.java/learn/numbers-strings/strings/). If not, please refer to the [Java documentation](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/lang/String.html) for more information.

## Strings in table operations (query strings)

<!-- TODO: Update link to relational operators #3812 -->

The first and foremost place strings get used in Deephaven queries is table operations. The strings used as inputs for table operations are typically called query strings. A query string contains a [formula](../how-to-guides/formulas-how-to.md), which either assigns data to or filters a column by relating the column to its values through the use of one or more [operators](../how-to-guides/formulas-how-to.md#operators).

The following code passes the query string `X = i` to [`update`](../reference/table-operations/select/update.md).

```groovy order=table
table = emptyTable(5).update("X = i")
```

`X = i` is a query string because it relates a column (`X`) to its values ([`i`](../reference/query-language/variables/special-variables.md)) by the [assignment operator](../how-to-guides/formulas-how-to.md#assignment-operators) `=`. Query strings either [create](./new-and-empty-table.md#create-new-columns-in-a-table), [modify](./drop-move-rename-columns.md), or [filter](./use-filters.md) data from tables. The following query [creates](./new-and-empty-table.md#create-new-columns-in-a-table) data and then [filters](./use-filters.md) it with query strings.

```groovy order=table
table = emptyTable(10).update("X = i").where("X % 2 == 1")
```

Methods like [`update`](../reference/table-operations/select/update.md) and [`where`](../reference/table-operations/filter/where.md) can take any number of strings as input (varargs) to pass in multiple query strings. The code below uses [`update`](../reference/table-operations/select/update.md) to add two new columns, `X` and `Y`, to an [empty table](../reference/table-operations/create/emptyTable.md).

```groovy order=table
table = emptyTable(10).update("X = i", "Y = String.valueOf(X)")
```

### Query strings and g-strings

Groovy's [gstrings](https://docs.groovy-lang.org/latest/html/api/groovy/lang/GString.html), unlike normal strings, allow for interpolation. In Groovy, gstrings are denoted by double quotes (`"`) and the use of `${}` inside the string. Gstrings can generate query strings programmatically. This approach can increase the readability of queries by dramatically reducing the amount of code required. The query below creates two identical tables with 10 columns, both with and without [gstrings](https://docs.groovy-lang.org/latest/html/api/groovy/lang/GString.html) to show the difference in the amount of code required.

```groovy order=sourceGstring,source
def myList = []

for (i = 0; i < 10; i++) {
    myList.add("X${i} = ${i} * i")
}

sourceGstring = emptyTable(10).update(*myList)

source = emptyTable(10).update(
    "X0 = 0 * i",
    "X1 = 1 * i",
    "X2 = 2 * i",
    "X3 = 3 * i",
    "X4 = 4 * i",
    "X5 = 5 * i",
    "X6 = 6 * i",
    "X7 = 7 * i",
    "X8 = 8 * i",
    "X9 = 9 * i",
)
```

### Strings in query strings

Formulas in query strings can contain strings of their own. Strings inside query strings are denoted by backticks (`` ` ``). The following code uses a query string that contains another string in the [`where`](../reference/table-operations/filter/where.md) operation.

```groovy order=result,source
source = newTable(stringCol("X", "A", "B", "B", "C", "B", "A", "B", "B", "C"))

result = source.where("X = `C`")
```

### String literals in query strings

Formulas in query strings can also make use of string literals. These are denoted by single quotes (`'`). String literals are different from normal [strings in query strings](#strings-in-query-strings) because they get interpreted differently. String literals can be inferred as another data type. The following example uses both a string and a string literal in query strings to show how they differ. The string literal gets inferred as a [Duration](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Duration.html) to add one second to the timestamp.

```groovy order=result,source
source = emptyTable(1).update("Now = now()")

result = source.update("NowPlusString = Now + `PT1s`", "NowPlusStringLiteral = Now + 'PT1s'")
```

## Strings in tables

In the following example, a [new table](../reference/table-operations/create/newTable.md) is created with two [string columns](../reference/table-operations/create/stringCol.md).

> [!NOTE]
> [String](../reference/query-language/types/strings.md) columns can be created using single or double quotes. Double quotes are recommended, especially when using [string literals](#string-literals-in-query-strings).

```groovy
result = newTable(
    stringCol("X", "A", "B", "B", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "M", "N", "N", "O", "O", "P", "P", "P"),
)
```

### String concatenation

String columns can be concatenated in queries. The following example adds two [[string columns](../reference/table-operations/create/stringCol.md)] together with `+`.

```groovy order=result,source
source = newTable(
    stringCol("X", "A", "B", "B", "C", "B", "A", "B", "B", "C"),
    stringCol("Y", "M", "M", "N", "N", "O", "O", "P", "P", "P"),
)

result = source.update("Add = X + Y")
```

### Escape characters

Strings in Deephaven support the escape character `\`. The escape character invokes alternative interpretations of the characters that follow it. For example, `\n` is a new line and not the character `n`. Similarly, `\t` is a tab and not the character `t`.

The query below shows how Deephaven responds to these characters.

```groovy order=result
result = newTable(
    stringCol("X", "Quote \" in quotes", 'Single quote \' in single quotes', "Escaped slash \\", "New\nline", "Added\ttab")
)
```

> [!NOTE]
> For more information on escaping characters in strings, see the [Python documentation](https://docs.python.org/3/reference/lexical_analysis.html#strings).

### String filters

Deephaven supports using [`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/lang/String.html) methods on [strings](../reference/query-language/types/strings.md) in queries.

The following example shows how to [filter](./use-filters.md) a [string column](../reference/table-operations/create/stringCol.md) for values that start with `"C"`.

```groovy order=source,result
source = newTable(stringCol("X", "Aa", "Ba", "Bb", "Ca", "Bc", "Ab", "Bd", "Be", "Cb"))

result = source.where("X.startsWith(`C`)")
```

## Related documentation

- [Create a new table](./new-and-empty-table.md#newtable)
- [Formulas](../how-to-guides/formulas-how-to.md)
- [query strings](../reference/query-language/types/strings.md)
