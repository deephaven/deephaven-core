---
title: String and char literals in query strings
sidebar_label: String & char
---

A literal value is one that is explicitly defined in the code rather than computed or derived from other values. In the query language, literals are commonly used to define constant values in expressions. This guide covers string and char literals in query strings. Since query strings use Java syntax for literals, they follow Java conventions, which differ from Python syntax.

## Char literals

In Java, a `char` is a primitive data type used to store a single character. This is different from Python, which doesn't have a dedicated character type. In Python, a single character is simply a string of length one.

In Java, `char` is a primitive, but a string is an object that represents a sequence of characters. Because `char` is a primitive type, it is generally more memory-efficient and computationally efficient than a string object for handling single characters.

Char literals in query strings are denoted by single quotes (`'`) around a single character. The following code block demonstrates this:

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(1).update(["CharLiteralOne = 'a'", "CharLiteralTwo = 'A'"])
source_meta = source.meta_table
```

## String literals

String literals in query strings contain zero or more characters and are enclosed in backticks (`` ` ``). For example, `` `Hello, world!` `` is a string literal. String literals can include any character, including spaces and special characters such as `\t`, `\n`, and `\"`.

The following query creates some columns in the `source` table using string literals:

```python order=source
from deephaven import empty_table

source = empty_table(1).update(
    [
        "StringLiteralColumn = `Hello, World!`",
        'StringWithQuotesColumn = `"This is a string with quotes!"`',
        "StringWithEscapeCharacter = `This\tcontains a tab`",
    ]
)
source_meta = source.meta_table
```

String literals can be concatenated with any other string via the `+` operator. The following example adds multiple string columns together:

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(1).update(
    [
        "NameOne = `Greg`",
        "Verb = ` supports `",
        "NameTwo = `Megan`",
        "Sentence = NameOne + Verb + NameTwo",
        "StringPlusChar = `Benjami` + 'n'",
    ]
)
source_meta = source.meta_table
```

## Related documentation

- [Boolean and numeric literals](./boolean-numeric-literals.md)
- [Date-time literals](./date-time-literals.md)
