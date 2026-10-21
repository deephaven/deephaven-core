---
title: Boolean and numeric literals in query strings
---

A literal value is one that is explicitly defined in code rather than computed or derived from other values. In the Deephaven Query Language (DQL), you use literals to define constant values in expressions. This guide covers boolean and numeric literals in [query strings](./query-string-overview.md).

[Query strings](./query-string-overview.md) use Java syntax for literals, which can differ from Python's. For example, the boolean literals are lowercase, and `0123` is an octal number. The engine also departs from Java in a few cases, described under [Numeric literals](#numeric-literals).

## Boolean literals

In DQL, there are only two boolean literal values: `true` and `false` (lowercase). Python capitalizes them as `True` and `False`, but a [query string](./query-string-overview.md) can't use that spelling. The engine reads `True` or `False` as the name of a variable or column, and the query fails if no variable or column has that name.

The following code uses both boolean literals, then shows the column types with [`meta_table`](../reference/table-operations/metadata/meta_table.md):

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(1).update(["TrueLiteral = true", "FalseLiteral = false"])
source_meta = source.meta_table
```

In `source_meta`, both columns are `java.lang.Boolean`, the type Deephaven uses for boolean columns.

## Numeric literals

Numeric literals in DQL can be integers or floating-point numbers. By default, the engine treats an integer literal as a 32-bit signed integer ([Java primitive](./data-types.md#primitive-types) `int`) and a floating-point literal as a 64-bit floating-point number ([Java primitive](./data-types.md#primitive-types) `double`).

Java rejects an integer literal that is too large for an `int`. The engine makes it a `long` instead. For a hexadecimal or binary literal, the engine compares the unsigned value with the `int` maximum, so `0xFFFFFFFF` is the `long` 4294967295, not the `int` -1 that Java produces.

A Python `int` variable follows different rules. It becomes the smallest Java integer type that holds its value, as described in [Python variables in query strings](./python-variables.md#scalars).

You can specify the type of a numeric literal by appending a suffix to the number:

- `L` or `l` for a 64-bit signed integer ([Java primitive](./data-types.md#primitive-types) `long`).
- `f` for a 32-bit floating-point number ([Java primitive](./data-types.md#primitive-types) `float`). The suffix must be lowercase. With an uppercase `F`, as in `3.14F`, the engine creates a `double` column that holds the `float`-rounded value 3.140000104904175.
- `d` or `D` for a 64-bit floating-point number ([Java primitive](./data-types.md#primitive-types) `double`). This suffix is optional on a literal that already has a decimal point or exponent, since `double` is the default there. On a whole number, `5d` is a `double`, while `5` is an `int`.

Numeric literals can also be written in these forms:

- **Hexadecimal**: prefix the digits with `0x` or `0X`, as in `0xABCD` (43981).
- **Binary**: prefix the digits with `0b` or `0B`, as in `0b1010` (10).
- **Octal**: start the digits with `0`, so `0123` is the octal number 83, not 123. Python users may read `0123` as decimal 123, because Python 3 writes octal with a `0o` prefix and rejects `0123` as a syntax error.
- **Scientific notation**: use `e` or `E` for a decimal exponent, as in `1.23e3` (1230.0).
- **Hexadecimal floating point**: use `p` or `P` for a binary exponent after `0x`-prefixed hexadecimal digits, as in `0x1.23p3` (9.09375).
- **Underscores**: place `_` between digits for readability, as in `1_000_000`. The underscores don't change the value.

The following example constructs a column for each default type, suffix, and form above, then shows the table metadata with [`meta_table`](../reference/table-operations/metadata/meta_table.md):

```python order=source,source_meta
from deephaven import empty_table

source = empty_table(1).update(
    [
        "IntLiteral = 5",
        "IntOctalLiteral = 0123",
        "IntHexadecimalLiteral = 0xABCD",
        "IntBinaryLiteral = 0b1010",
        "LongLiteral = 123456789L",
        "PromotedLongLiteral = 3000000000",
        "PromotedHexadecimalLiteral = 0xFFFFFFFF",
        "FloatLiteral = 3.14f",
        "DoubleLiteral = 3.14",
        "DoubleSuffixLiteral = 5d",
        "FloatScientificLiteral = 1.23e3f",
        "DoubleScientificLiteral = 1.23e3",
        "FloatHexadecimalLiteral = 0x1.23p3f",
        "DoubleHexadecimalLiteral = 0x1.23p3",
        "IntUnderscoreLiteral = 1_000_000",
        "LongUnderscoreLiteral = 500_000_000L",
        "FloatUnderscoreLiteral = 9_876.543_210f",
        "DoubleUnderscoreLiteral = 1_232.815_679",
    ]
)
source_meta = source.meta_table
```

In `source_meta`, each column's type follows the rules above. `PromotedLongLiteral` and `PromotedHexadecimalLiteral` have no suffix, but they are `long` columns because their values are too large for an `int`. `DoubleSuffixLiteral` is a `double` column. In `source`, `PromotedHexadecimalLiteral` holds 4294967295 and `IntOctalLiteral` holds 83.

## Related documentation

- [Query string overview](./query-string-overview.md)
- [String and char literals](./string-char-literals.md)
- [Date-time literals](./date-time-literals.md)
- [Data types](./data-types.md)
- [Casting](./casting.md)
- [Python variables in query strings](./python-variables.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`meta_table`](../reference/table-operations/metadata/meta_table.md)
- [`update`](../reference/table-operations/select/update.md)
