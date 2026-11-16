---
title: String and char literals in query strings
---

A literal value is one that is explicitly defined in the code rather than computed or derived from other values. In the query language, literals commonly define constant values in expressions.

This guide covers char literals and string literals in [query strings](./query-string-overview.md). Query strings use Java syntax for literals, so they follow Java conventions, which differ from Python's. A char literal is one character in single quotes, such as `'a'`. A string literal goes in backticks, a Deephaven addition that stands in for Java's double quotes.

## Char literals

A char literal holds exactly one character between single quotes (`'`).

In Java, a [`char`](./data-types.md#primitive-types) is a primitive data type that stores a single character, while a [string](../reference/query-language/types/strings.md) is an object that represents a sequence of characters. Because [`char`](./data-types.md#primitive-types) is a primitive type, it uses less memory and is faster to process than a [string](../reference/query-language/types/strings.md) object that holds one character. Python has no dedicated character type: a single Python character is a string of length one.

The following code block creates two char columns. The `source_meta` table shows that each column's data type is [`char`](./data-types.md#primitive-types):

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(1).update(["CharLiteralOne = 'a'", "CharLiteralTwo = 'A'"])
source_meta = source.meta_table
```

Unlike in Python, you can't put a [string](../reference/query-language/types/strings.md) in single quotes in a [query string](./query-string-overview.md). Deephaven reads single-quoted text longer than one character as a [date-time literal](./date-time-literals.md), and the query fails if the text isn't a valid date-time value. Use backticks for [strings](../reference/query-language/types/strings.md), as described in [String literals](#string-literals).

Char literals in query strings can't hold Java escape sequences such as `\t`. The backslash reaches the query language when the query string is a raw string or when you write `'\\t'`. Then `'\t'` is two characters between the quotes, so Deephaven reads it as a [date-time literal](./date-time-literals.md) and the query fails. To get a tab or newline, use an escape sequence in a [string literal](#string-literals) instead.

## String literals

[String](../reference/query-language/types/strings.md) literals in [query strings](./query-string-overview.md) contain zero or more characters inside backticks (`` ` ``). For example, `` `Hello, World!` `` is a string literal.

### Quote a string literal

Deephaven converts backticks to Java double quotes before it compiles the [query string](./query-string-overview.md). [Query strings](./query-string-overview.md) also accept Java double quotes directly, so you can write a string literal in a Python [query string](./query-string-overview.md) in two ways:

- Backticks inside a Python double-quoted string, such as ``"Greeting = `Hello`"``. No quotes need escaping.
- Java double quotes inside a Python single-quoted string, such as `'Greeting = "Hello"'`.

### Quotes and escape sequences in a string literal

A backtick literal can contain double quotes. Deephaven escapes them when it converts the backticks. In Python, though, a double quote would end a double-quoted [query string](./query-string-overview.md). Put such a query string in Python single quotes, as `StringWithQuotesColumn` does in the example below.

String literals can contain Java escape sequences such as `\t` (tab) and `\n` (newline). Because the [query string](./query-string-overview.md) is itself a Python string, Python turns `\n` into a real line break before Deephaven sees it. A Java string literal can't contain a line break, so the query fails. Escape the backslash (`\\n`) or use a raw string (`r"..."`) so the escape sequence reaches the query language.

The following query creates three string columns in the `source` table:

- `StringLiteralColumn` holds a plain string literal.
- `StringWithQuotesColumn` holds a literal that contains double quotes.
- `StringWithEscapeCharacter` holds a literal with a tab. Its raw [query string](./query-string-overview.md) lets `\t` reach the query language as an escape sequence.

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(1).update(
    [
        "StringLiteralColumn = `Hello, World!`",
        'StringWithQuotesColumn = `"This is a string with quotes!"`',
        r"StringWithEscapeCharacter = `This\tcontains a tab`",
    ]
)
source_meta = source.meta_table
```

### Apostrophes in a string literal

Deephaven looks for [date-time literals](./date-time-literals.md) before it reads backticks, and it doesn't skip string literals while it looks. It reads any two apostrophes in a [formula](./formulas.md) with two or more characters between them as a [date-time literal](./date-time-literals.md). As a result:

- A string literal with one apostrophe, such as `` `It's` ``, works only when no other apostrophe appears in the same [formula](./formulas.md).
- A string literal with two apostrophes, such as `` `Bob's and Al's` ``, fails, because Deephaven tries to parse `s and Al` as a date-time value.
- A string literal that puts a valid date-time value between apostrophes, such as `` `Meet at '10:00' today` ``, runs, but the string holds a generated name such as `_localTime0` in place of `'10:00'`.

To use text with apostrophes, store it in a [Python variable](./python-variables.md) and refer to the variable in the [query string](./query-string-overview.md).

In the following example, the `text` variable holds two apostrophes. The apostrophes are in the value of `text`, not in the [query string](./query-string-overview.md), so the search for [date-time literals](./date-time-literals.md) never sees them. `Message` holds `Bob's and Al's`:

```python order=source
from deephaven import empty_table

text = "Bob's and Al's"
source = empty_table(1).update(["Message = text"])
```

## Concatenate string and char literals

Use the [`+` operator](./operators.md#arithmetic-operators) to concatenate [strings](../reference/query-language/types/strings.md) and [chars](./data-types.md#primitive-types) in a [query string](./query-string-overview.md), whether they are literals or columns. [`+`](./operators.md#arithmetic-operators) is evaluated left to right, and each [`+`](./operators.md#arithmetic-operators) concatenates only when one of its two operands is a [string](../reference/query-language/types/strings.md).

Two [chars](./data-types.md#primitive-types) add as numbers. [`+`](./operators.md#arithmetic-operators) adds their numeric character codes and returns an [`int`](./data-types.md#primitive-types), so `'a' + 'b'` is `195` and `` 'a' + 'b' + `!` `` gives `195!`. To get `ab`, start the expression with a [string](../reference/query-language/types/strings.md): `+ 'a' + 'b'`.

The following example builds `Sentence` from three columns that hold string literals, builds `StringPlusChar` by concatenating a string literal directly with a char literal, and builds `CharPlusChar` from two char literals:

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(1).update(
    [
        "NameOne = `Greg`",
        "Verb = ` supports `",
        "NameTwo = `Megan`",
        "Sentence = NameOne + Verb + NameTwo",
        "StringPlusChar = `Benjami` + 'n'",
        "CharPlusChar = 'a' + 'b'",
    ]
)
source_meta = source.meta_table
```

In the `source` table, `CharPlusChar` holds `195`, not `ab`. The `source_meta` table shows that its type is [`int`](./data-types.md#primitive-types), while `StringPlusChar` is a [`String`](../reference/query-language/types/strings.md).

## Related documentation

- [Query string overview](./query-string-overview.md)
- [Formulas](./formulas.md)
- [Operators](./operators.md)
- [Work with strings](./work-with-strings.md)
- [Boolean and numeric literals](./boolean-numeric-literals.md)
- [Date-time literals](./date-time-literals.md)
- [Data types](./data-types.md)
- [Python variables in query strings](./python-variables.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`meta_table`](../reference/table-operations/metadata/meta_table.md)
- [`update`](../reference/table-operations/select/update.md)
- [Strings](../reference/query-language/types/strings.md)
- [`empty_table` Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.empty_table)
- [`meta_table` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.meta_table)
- [`update` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.update)
