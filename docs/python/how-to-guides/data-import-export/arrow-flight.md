---
title: Arrow Flight and Deephaven
sidebar_label: Arrow Flight
---

[Apache Arrow](https://arrow.apache.org/) is a high-performance language-independent columnar memory format for flat and nested data. Deephaven's Arrow Flight integration enables users to import and export data via native remote Flight clients over gRPC.

## Unsupported Types / Features

- Maps have very limited support as Deephaven Column Sources cannot keep track of key or value types.
- No support for large types such as LargeUtf8, LargeBinary, LargeList, and LargeListView (requires simulating 64-bit arrays).
- No support for structs.
- No support for run-end encoding.
- No support for dictionary encoding.
- No support for Utf8View or BinaryView.

## Arrow Type Support Matrix

Deephaven supports a wide range of Arrow types. The following table summarizes the supported Arrow types and their corresponding Deephaven types. The table also includes notes on coercions and other relevant information.

| Arrow Type                            | Default Deephaven Type                                                          | Supported Coercions                                                            | Notes                                                                                                                                                                                        |
| ------------------------------------- | ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Bool`                                | `Boolean`                                                                       | `byte`, `boolean`                                                              | Primitive `boolean` is typically automatically boxed to allow for nulls, but may be targeted for array types.                                                                                |
| `Int(8, signed)`                      | `byte`                                                                          | `char`, `short`, `int`, `long`, `float`, `double`, `BigInteger`, `BigDecimal`  | See notes on [integral coercion](#integral-coercion).                                                                                                                                        |
| `Int(8, unsigned)`                    | `short`                                                                         | `byte`, `char`, `int`, `long`, `float`, `double`, `BigInteger`, `BigDecimal`   | See notes on [integral coercion](#integral-coercion).                                                                                                                                        |
| `Int(16, signed)`                     | `short`                                                                         | `byte`, `char`, `int`, `long`, `float`, `double`, `BigInteger`, `BigDecimal`   | See notes on [integral coercion](#integral-coercion).                                                                                                                                        |
| `Int(16, unsigned)`                   | `char`                                                                          | `byte`, `short`, `int`, `long`, `float`, `double`, `BigInteger`, `BigDecimal`  | See notes on [integral coercion](#integral-coercion).                                                                                                                                        |
| `Int(32, signed)`                     | `int`                                                                           | `byte`, `char`, `short`, `long`, `float`, `double`, `BigInteger`, `BigDecimal` | See notes on [integral coercion](#integral-coercion).                                                                                                                                        |
| `Int(32, unsigned)`                   | `long`                                                                          | `byte`, `char`, `short`, `int`, `float`, `double`, `BigInteger`, `BigDecimal`  | See notes on [integral coercion](#integral-coercion).                                                                                                                                        |
| `Int(64, signed)`                     | `long`                                                                          | `byte`, `char`, `short`, `int`, `float`, `double`, `BigInteger`, `BigDecimal`  | See notes on [integral coercion](#integral-coercion).                                                                                                                                        |
| `Int(64, unsigned)`                   | `BigInteger`                                                                    | `byte`, `char`, `short`, `int`, `long`, `float`, `double`, `BigDecimal`        | See notes on [integral coercion](#integral-coercion).                                                                                                                                        |
| `Decimal`                             | `BigDecimal`                                                                    | `byte`, `char`, `short`, `int`, `long`, `float`, `double`, `BigInteger`        | See notes on [decimal coercion](#decimal-coercion).                                                                                                                                          |
| `FloatingPoint(precision)`            | `float` (for HALF/SINGLE) or `double` (for DOUBLE)                              | `byte`, `char`, `short`, `int`, `long`, `BigDecimal`, `BigInteger`             | See notes on [decimal coercion](#decimal-coercion).                                                                                                                                          |
| `Time(timeUnit, bitWidth)`            | `LocalTime`                                                                     | —                                                                              | Time-of-day values; bit width (32 or 64) depends on the time unit.                                                                                                                           |
| `Timestamp(timeUnit, [timeZone])`     | `Instant` (or `ZonedDateTime` if non-UTC timezone provided)                     | `long`, `LocalDateTime`                                                        | It is assumed that the unit of a `long` column, being treated as a `Timestamp`, is in nanoseconds. On the wire, this value is adjusted to match the field's TimeUnit.                        |
| `Date(dateUnit)`                      | `LocalDate`                                                                     | —                                                                              |                                                                                                                                                                                              |
| `Duration(timeUnit)`                  | `Duration`                                                                      | —                                                                              |                                                                                                                                                                                              |
| `Interval(intervalUnit)`              | `Period` (YEAR_MONTH), `Duration` (DAY_TIME), `PeriodDuration` (MONTH_DAY_NANO) | May allow limited cross‑coercion between interval types.                       | Deephaven's implementation matches the Arrow-Java implementation, using Flight's `PeriodDuration`, for this column type.                                                                     |
| `Utf8`                                | `String`                                                                        | —                                                                              | Text encoded as UTF‑8.                                                                                                                                                                       |
| `Binary`                              | `byte[]`                                                                        | `ByteVector`, `ByteBuffer`                                                     | [Custom mappings](#further-customization) are supported. Deephaven has custom encodings for BigDecimal, BigInteger, and Arrow Flight's Schema POJO.                                          |
| `FixedSizeBinary(fixedLength)`        | `byte[]`                                                                        | `ByteVector, ByteBuffer`                                                       | [Custom mappings](#further-customization) are supported.                                                                                                                                     |
| `SparseUnion`/`DenseUnion`            | `Object`                                                                        | —                                                                              | Union types (dense or sparse) map to a generic `Object` column type.                                                                                                                         |
| `Null`                                | `Object`                                                                        | —                                                                              | Best when coupled with a Union or when [specifying the Deephaven type to use](#further-customization).                                                                                       |
| `Map`                                 | `LinkedHashMap`                                                                 | —                                                                              | Map types require that the [Arrow schema is specified](#further-customization). Cells are accumulated into `LinkedHashMap`s; it's not possible to pick another collector at this time.       |
| `List` / `ListView` / `FixedSizeList` | `T[]` - a native Java array of the inner field                                  | Supports conversion to Deephaven internal `Vector` types such as `LongVector`. | Note that the `deephaven:componentType` metadata key must be set to the inner type. Fixed-size list wire formats will be truncated and/or null-padded to match the data as best as possible. |

### Integral Coercion

Integral coercion from a larger type to a smaller type follows typical truncation rules, however, some values will be "truncated" to `null` values. For example `0x2080` will be truncated to `null` when coerced to a `byte` value as `QueryConstants.NULL_BYTE` is `0x80`. Similarly, any `-1` will be coerced to `null` when coerced to a `char` as `QueryConstants.NULL_CHAR` is `0xFFFF`.

### Decimal Coercion

Decimal coercion from a type with higher precision to a type with lower precision may result in rounding or truncation. For example, coercion to any integral type will lose the fractional part of the decimal value.

## Further customization

Please see the [Groovy documentation](/core/groovy/docs/how-to-guides/data-import-export/arrow-flight) for more details and examples.

## Related documentation

- [Barrage Extensions Package Summary](https://docs.deephaven.io/core/javadoc/io/deephaven/extensions/barrage/package-summary.html)
- [Arrow Flight Package Summary](https://docs.deephaven.io/core/javadoc/org/apache/arrow/flight/package-summary.html)
