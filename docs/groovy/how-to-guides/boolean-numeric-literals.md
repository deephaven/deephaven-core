---
title: Boolean and numeric literals in query strings
sidebar_label: Boolean & numeric
---

A literal value is one that is explicitly defined in code rather than computed or derived from other values. In the query language, literals are commonly used to define constant values in expressions. This guide covers boolean and numeric literals in query strings. Since query strings use Java syntax for literals, they follow Java conventions, which differ from Python syntax.

## Boolean literals

The Deephaven Query Language (DQL) uses `true` and `false` literal values:

```groovy order=source,sourceMeta
source = emptyTable(1).update("TrueLiteral = true", "FalseLiteral = false")
sourceMeta = source.meta()
```

## Numeric literals

Numeric literals in the query language can be integers or floating-point numbers. By default, an integer literal is treated as a 32-bit signed integer (`int`), and a floating-point literal is treated as a 64-bit floating-point number (`double`). You can specify the type of a numeric literal by appending a suffix to the number:

- `L` or `l` for a 64-bit signed integer (Java primitive `long`).
- `f` for a 32-bit floating-point number (Java primitive `float`).

The following example constructs four columns, each with a numeric literal. The table metadata is also shown:

```groovy order=source,sourceMeta
source = emptyTable(1).update(
    "IntLiteral = 5",
    "IntOctalLiteral = 0123",
    "IntHexadecimalLiteral = 0xABCD",
    "IntBinaryLiteral = 0b1010",
    "LongLiteral = 123456789L",
    "FloatLiteral = 3.14f",
    "DoubleLiteral = 3.14",
    "FloatScientificLiteral = 1.23e3f",
    "DoubleScientificLiteral = 1.23e3",
    "FloatHexadecimalLiteral = 0x1.23p3f",
    "DoubleHexadecimalLiteral = 0x1.23p3",
    "IntUnderscoreLiteral = 1_000_000",
    "LongUnderscoreLiteral = 500_000_000L",
    "FloatUnderscoreLiteral = 9_876.543_210f",
    "DoubleUnderscoreLiteral = 1_232.815_679",
)
sourceMeta = source.meta()
```

## Related documentation

- [String and char literals](./string-char-literals.md)
- [Date-time literals](./date-time-literals.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`meta`](../reference/table-operations/metadata/meta.md)
