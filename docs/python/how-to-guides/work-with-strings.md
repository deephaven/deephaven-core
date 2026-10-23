---
title: Strings
---

A [string](https://en.wikipedia.org/wiki/String_(computer_science)) is a sequence of characters, such as letters, numbers, or symbols, that a program handles as a single value. Deephaven queries use strings for column values, [filters](./filters.md), and [formulas](./formulas.md).

The Deephaven engine represents strings as [`java.lang.String`](../reference/query-language/types/strings.md). It converts [Python strings](./python-variables.md#strings) for you, so your Python string variables work in queries with no extra steps.

This guide shows how to use Python strings and Java strings in [query strings](./query-string-overview.md).

## Python strings in queries

You can use [Python string variables](./python-variables.md#strings) in [query strings](./query-string-overview.md) directly. The engine converts them to [`java.lang.String`](../reference/query-language/types/strings.md) objects.

Inside a [query string](./query-string-overview.md), a [string literal](./string-char-literals.md#string-literals) usually goes between backticks, which avoids escaping double quotes.

Let's add the literal `` `Hello, ` `` to the variable `my_str`. The `source_meta` table, from [`meta_table`](../reference/table-operations/metadata/meta_table.md), lists each column's name and type. Its `DataType` column shows `java.lang.String`:

```python order=source,source_meta
from deephaven import empty_table

my_str = "Deephaven!"

source = empty_table(1).update("MyStringColumn = `Hello, ` + my_str")
source_meta = source.meta_table
```

[Python functions](./python-functions.md) that return strings work too, as long as they have a `str` return [type hint](./python-functions.md#type-hints). The hint tells the engine to make a [`String`](../reference/query-language/types/strings.md) column:

```python order=source,source_meta
from deephaven import empty_table


def string_function() -> str:
    return "Hello, Deephaven!"


source = empty_table(1).update("StringColumn = string_function()")
source_meta = source.meta_table
```

Without the hint, the column doesn't hold strings. It holds [`PyObject`](./pyobjects.md) or `java.lang.Object` values instead.

## Java strings in queries

Deephaven stores string columns as [`java.lang.String`](../reference/query-language/types/strings.md) objects, so you work with them in [query strings](./query-string-overview.md) the way Java does:

- String concatenation with the `+` [operator](./operators.md), which concatenates when either operand is a string
- Native [`java.lang.String`](../reference/query-language/types/strings.md) methods, such as [`indexOf`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html#indexOf(java.lang.String)) and [`substring`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html#substring(int,int))
- Deephaven's [built-in functions](./built-in-functions.md) that work with strings

### String concatenation

The `+` [operator](./operators.md) concatenates [`java.lang.String`](../reference/query-language/types/strings.md) objects, just as it concatenates Python strings:

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(1).update(
    ["StrCol1 = `Hello, `", "StrCol2 = `Deephaven!`", "StrCol3 = StrCol1 + StrCol2"]
)
source_meta = source.meta_table
```

### String methods

You can call [`java.lang.String`](../reference/query-language/types/strings.md) methods directly from [query strings](./query-string-overview.md) to search, slice, and change the case of text without leaving the query. Because the column holds [`java.lang.String`](../reference/query-language/types/strings.md) values, use Java method names such as `toUpperCase`, not Python ones such as `upper`:

```python order=source,result
from deephaven import empty_table

source = empty_table(1).update(
    [
        "StrCol1 = `Learning `",
        "StrCol2 = `about `",
        "StrCol3 = `strings.`",
        "StrCol4 = StrCol1 + StrCol2 + StrCol3",
    ]
)
result = source.update(
    [
        "IndexOfSubstring = StrCol4.indexOf(`ings.`)",
        "StringIsEmpty = StrCol2.isEmpty()",
        "Substring = StrCol4.substring(4, 14)",
        "UpperCase = StrCol1.toUpperCase()",
        "LowerCase = StrCol3.toLowerCase()",
    ]
)
```

In `Learning about strings.`, the text `ings.` starts at index 18, so `IndexOfSubstring` is `18`. `Substring` is `ning about`, the characters from index 4 up to but not including index 14.

#### String filtering

A common use of string methods is [string filtering](./use-filters.md#string-filters):

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col

source = new_table(
    [string_col("X", ["Aa", "Ba", "Bb", "Ca", "Bc", "Ab", "Bd", "Be", "Cb"])]
)

result = source.where(filters=["X.startsWith(`C`)"])
```

`result` keeps only the rows whose `X` value starts with `C`: `Ca` and `Cb`.

### Built-in functions

Many [built-in functions](./built-in-functions.md) work with strings. For instance, the [parse functions](../reference/query-language/query-library/auto-imported/parse.md) convert strings to numeric and boolean values:

```python order=source,result,result_meta
from deephaven import empty_table

source = empty_table(1).update(
    [
        "StringInt = `4`",
        "StringFloat = `4.5`",
        "StringDouble = `3.14159`",
        "StringBool = `true`",
    ]
)
result = source.update(
    [
        "Int = parseInt(StringInt)",
        "Float = parseFloat(StringFloat)",
        "Double = parseDouble(StringDouble)",
        "Bool = parseBoolean(StringBool)",
    ]
)
result_meta = result.meta_table
```

`result_meta` shows that `Int` is an `int` column, `Float` a `float` column, `Double` a `double` column, and `Bool` a `java.lang.Boolean` column.

## Related documentation

- [Filter table data](./use-filters.md)
- [Create static tables](./new-and-empty-table.md)
- [An overview of query strings](./query-string-overview.md)
- [String and char literals in query strings](./string-char-literals.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [Handle PyObjects in tables](./pyobjects.md)
- [Java objects and classes in query strings](./java-classes.md)
- [Built-in query language functions](./built-in-functions.md)
- [Filters](./filters.md)
- [Formulas](./formulas.md)
- [Operators](./operators.md)
- [Strings](../reference/query-language/types/strings.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`string_col`](../reference/table-operations/create/stringCol.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`meta_table`](../reference/table-operations/metadata/meta_table.md)
- [Parse functions](../reference/query-language/query-library/auto-imported/parse.md)
- [`empty_table` Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.empty_table)
- [`new_table` Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.new_table)
- [`string_col` Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.string_col)
- [`update` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.update)
- [`where` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.where)
- [`meta_table` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.meta_table)
