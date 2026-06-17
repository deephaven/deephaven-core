---
title: Strings
---

A [String](https://en.wikipedia.org/wiki/String_(computer_science)) is a sequence of characters, such as letters, numbers, or symbols, that is treated as a single data type. Strings frequently contain words or text. Deephaven queries use strings ubiquitously. The Deephaven engine uses [`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html) but is designed to interoperate with Python strings as well.

This guide focuses on the use of both Python and Java strings interchangeably in queries.

## Python strings in queries

Python strings are analogous to Java strings, as they share many similarities. [Python string variables](./python-variables.md) can be passed into table operations, and the engine knows how to handle them without any additional input. Note the column types. The Python strings are automatically converted to Java [`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html) strings.

```python order=source,source_meta
from deephaven import empty_table

my_str = "Deephaven!"

source = empty_table(1).update("MyStringColumn = `Hello, ` + my_str")
source_meta = source.meta_table
```

[Python functions](./python-functions.md) that return strings are no different:

```python order=source,source_meta
from deephaven import empty_table


def string_function() -> str:
    return "Hello, Deephaven!"


source = empty_table(1).update("StringColumn = string_function()")
source_meta = source.meta_table
```

## Java strings in queries

String columns in Deephaven tables are stored as [`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html) objects. This provides three powerful ways to manipulate text in [query strings](./query-string-overview.md):

1. Native Java string methods (like `length`, `substring`, etc.)
2. Deephaven's [built-in string functions](./built-in-functions.md)
3. String-specific [operators](./operators.md) (such as `+` for concatenation)

### String concatenation

Like Python strings, [`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html) objects can be concatenated with the `+` [operator](./operators.md):

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(1).update(
    ["StrCol1 = `Hello, `", "StrCol2 = `Deephaven!`", "StrCol3 = StrCol1 + StrCol2"]
)
source_meta = source.meta_table
```

### String methods

[`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html) methods can be called from [query strings](./query-string-overview.md) and can yield very powerful queries:

```python order=source,result
from deephaven import empty_table

source = empty_table(1).update(
    [
        "StrCol1 = `Learning `",
        "StrCol2 = `about `",
        "StrCol3 = ` strings.`",
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

### String filtering

A common use of string methods is for [string filtering](./use-filters.md#string-filters):

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col

source = new_table(
    [string_col("X", ["Aa", "Ba", "Bb", "Ca", "Bc", "Ab", "Bd", "Be", "Cb"])]
)

result = source.where(filters=["X.startsWith(`C`)"])
```

### Built-in functions

Many [built-in functions](./built-in-functions.md) work with strings. For instance, you can parse the numeric values in strings:

```python order=result,result_meta,source
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

## Related documentation

- [Filter table data](./use-filters.md)
- [New and empty table](./new-and-empty-table.md#new_table)
- [Query strings](./query-string-overview.md)
- [String literals in query strings](./string-char-literals.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [Java objects in query strings](./java-classes.md)
- [Filters](./filters.md)
- [Formulas](./formulas.md)
- [Operators in query strings](./operators.md)
