---
title: Arrow Flight and Deephaven
---

[Apache Arrow](https://arrow.apache.org/) is a high-performance, language-independent columnar memory format for flat and nested data. Deephaven's [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) integration lets you import and export data with remote Flight clients over gRPC. Deephaven's [Barrage](../../conceptual/what-is-barrage.md) protocol extends Flight, so Flight clients can also subscribe to ticking tables and receive only what changes each cycle.

This guide shows how Arrow types map to Deephaven column types, how to choose the Arrow types a table uses on the network (its _wire types_) when you import or export it, and where to learn how to register your own type mappings.

## Arrow type support matrix

Deephaven supports a wide range of Arrow types. The following table lists each supported Arrow type, the Deephaven type it maps to by default, and its supported coercions. Deephaven column types are Java types. For example, `long` is a Java primitive, and `Instant` is the Java class `java.time.Instant`. A coercion is a non-default Deephaven type that Deephaven can convert the Arrow type to on import, and convert back to the Arrow type on export. You request one on import with the [`deephaven:type`](#import) metadata tag, and on export by [attaching a Flight schema to the table](#export).

| Arrow Type                            | Default Deephaven Type                                                                | Supported Coercions                                                                                                         | Notes                                                                                                                                                                                                                                                                                               |
| ------------------------------------- | ------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Bool`                                | `Boolean`                                                                             | `byte`, `boolean`                                                                                                           | Deephaven uses boxed `Boolean` so that cells can hold nulls. A non-nullable field with no [`deephaven:type`](#import) tag defaults to primitive `boolean`, as does the component type of a non-nullable list element. On import, a `deephaven:type` of `boolean` still produces a `Boolean` column. |
| `Int(8, signed)`                      | `byte`                                                                                | `char`, `short`, `int`, `long`, `float`, `double`, `BigInteger`, `BigDecimal`                                               | See notes on [integral coercion](#integral-coercion).                                                                                                                                                                                                                                               |
| `Int(8, unsigned)`                    | `short`                                                                               | `byte`, `char`, `int`, `long`, `float`, `double`, `BigInteger`, `BigDecimal`                                                | See notes on [integral coercion](#integral-coercion).                                                                                                                                                                                                                                               |
| `Int(16, signed)`                     | `short`                                                                               | `byte`, `char`, `int`, `long`, `float`, `double`, `BigInteger`, `BigDecimal`                                                | See notes on [integral coercion](#integral-coercion).                                                                                                                                                                                                                                               |
| `Int(16, unsigned)`                   | `char`                                                                                | `byte`, `short`, `int`, `long`, `float`, `double`, `BigInteger`, `BigDecimal`                                               | See notes on [integral coercion](#integral-coercion).                                                                                                                                                                                                                                               |
| `Int(32, signed)`                     | `int`                                                                                 | `byte`, `char`, `short`, `long`, `float`, `double`, `BigInteger`, `BigDecimal`                                              | See notes on [integral coercion](#integral-coercion).                                                                                                                                                                                                                                               |
| `Int(32, unsigned)`                   | `long`                                                                                | `byte`, `char`, `short`, `int`, `float`, `double`, `BigInteger`, `BigDecimal`                                               | See notes on [integral coercion](#integral-coercion).                                                                                                                                                                                                                                               |
| `Int(64, signed)`                     | `long`                                                                                | `byte`, `char`, `short`, `int`, `float`, `double`, `BigInteger`, `BigDecimal`                                               | See notes on [integral coercion](#integral-coercion).                                                                                                                                                                                                                                               |
| `Int(64, unsigned)`                   | `BigInteger`                                                                          | `byte`, `char`, `short`, `int`, `long`, `float`, `double`, `BigDecimal`                                                     | See notes on [integral coercion](#integral-coercion).                                                                                                                                                                                                                                               |
| `Decimal`                             | `BigDecimal`                                                                          | `byte`, `char`, `short`, `int`, `long`, `float`, `double`, `BigInteger`                                                     | See notes on [decimal coercion](#decimal-coercion).                                                                                                                                                                                                                                                 |
| `FloatingPoint(precision)`            | `float` (for `HALF`/`SINGLE`) or `double` (for `DOUBLE`)                              | `byte`, `char`, `short`, `int`, `long`, `BigDecimal`, `BigInteger`, and whichever of `float` or `double` is not the default | See notes on [decimal coercion](#decimal-coercion).                                                                                                                                                                                                                                                 |
| `Time(timeUnit, bitWidth)`            | `LocalTime`                                                                           | —                                                                                                                           | Time-of-day values. The bit width, 32 or 64, depends on the time unit.                                                                                                                                                                                                                              |
| `Timestamp(timeUnit, [timeZone])`     | `Instant`, or `ZonedDateTime` when the field has a non-UTC time zone                  | `long`, `LocalDateTime`, and whichever of `Instant` or `ZonedDateTime` is not the default                                   | Deephaven reads a `long` column mapped to `Timestamp` as nanoseconds and converts each value to the field's `TimeUnit` on the wire.                                                                                                                                                                 |
| `Date(dateUnit)`                      | `LocalDate`                                                                           | —                                                                                                                           |                                                                                                                                                                                                                                                                                                     |
| `Duration(timeUnit)`                  | `Duration`                                                                            | —                                                                                                                           |                                                                                                                                                                                                                                                                                                     |
| `Interval(intervalUnit)`              | `Period` (`YEAR_MONTH`), `Duration` (`DAY_TIME`), `PeriodDuration` (`MONTH_DAY_NANO`) | `Period` (any unit), `PeriodDuration` (any unit), `Duration` (`DAY_TIME` only)                                              | `PeriodDuration` is Arrow Java's class. Deephaven uses it to match the Arrow Java implementation.                                                                                                                                                                                                   |
| `Utf8`                                | `String`                                                                              | —                                                                                                                           | Text encoded as UTF-8.                                                                                                                                                                                                                                                                              |
| `Binary`                              | `byte[]`                                                                              | `ByteVector`, `ByteBuffer`, `String`                                                                                        | [Custom mappings](#custom-mappings) are supported. Deephaven has custom encodings for `BigDecimal`, `BigInteger`, and Arrow Java's `Schema` object.                                                                                                                                                 |
| `FixedSizeBinary(fixedLength)`        | `byte[]`                                                                              | `ByteVector`, `ByteBuffer`                                                                                                  | [Custom mappings](#custom-mappings) are supported.                                                                                                                                                                                                                                                  |
| `SparseUnion`/`DenseUnion`            | `Object`                                                                              | —                                                                                                                           | Union types (dense or sparse) map to a generic `Object` column type.                                                                                                                                                                                                                                |
| `Null`                                | `Object`                                                                              | —                                                                                                                           | Best when coupled with a `Union` or when [specifying the Deephaven type to use](#import).                                                                                                                                                                                                           |
| `Map`                                 | `Map`                                                                                 | —                                                                                                                           | Deephaven collects each cell into a `LinkedHashMap`. You can't choose a different collector. A Deephaven column doesn't record the key or value types of its maps, so Deephaven exports a `Map` column as strings unless you [attach a Flight schema for the export](#export).                      |
| `List` / `ListView` / `FixedSizeList` | `T[]` — a native Java array of the inner field's default type                         | Other array types, such as `int[]`, and `Vector` types, such as `LongVector` and `ObjectVector`                             | Deephaven truncates or null-pads each value to fit a fixed-size list wire format. To choose the array or vector type on import, see [Import](#import).                                                                                                                                              |
| `RunEndEncoded`                       | Default type of the values child                                                      | —                                                                                                                           | The column type comes from the values child; the `run_ends` child is only an index.                                                                                                                                                                                                                 |
| Dictionary-encoded fields             | Default type of the dictionary value type                                             | —                                                                                                                           | Applies to any field that carries a `DictionaryEncoding`.                                                                                                                                                                                                                                           |

### Integral coercion

Coercion from a larger integral type to a smaller one, or from an unsigned type to the signed type of the same width, follows Java's narrowing primitive conversion, which keeps only the low-order bits. Some values truncate to `null`, because Deephaven reserves one value of each primitive type as its null marker. For example, `0x2080` becomes `null` when coerced to a `byte`, because [`QueryConstants.NULL_BYTE`](https://docs.deephaven.io/core/javadoc/io/deephaven/util/QueryConstants.html#NULL_BYTE) is `0x80`. Similarly, `-1` becomes `null` when coerced to a `char`, because [`QueryConstants.NULL_CHAR`](https://docs.deephaven.io/core/javadoc/io/deephaven/util/QueryConstants.html#NULL_CHAR) is `0xFFFF`.

### Decimal coercion

Coercing a decimal value to a type with lower precision rounds or truncates it. For example, coercion to any integral type drops the fractional part of the decimal value.

## Unsupported Arrow types

Deephaven does not support the following Arrow types:

- Large types such as `LargeUtf8`, `LargeBinary`, `LargeList`, and `LargeListView`
- `Struct`
- `Utf8View` and `BinaryView`

Supporting the large types would require simulating 64-bit arrays.

## Set the Flight schema

### Import

When a Flight client uploads data to Deephaven with a `DoPut` call, the client can specify the destination column types by adding metadata tags to the Flight schema's fields.

The metadata keys are as follows:

- `deephaven:type`: the canonical class name of the Deephaven type to use for the column. For a list field, this can be an array class, such as `int[]`, or a vector class, such as `io.deephaven.vector.LongVector`.
- `deephaven:componentType`: the canonical class name of the element type to use for the column. For an `ObjectVector` column, set it to choose the element class. Without it, Deephaven uses the default type of the list's element field. For an array column or a primitive vector column, Deephaven infers the element type from `deephaven:type`.

Any field without type tags gets the default Deephaven type for its Arrow wire type, as listed in the [Arrow type support matrix](#arrow-type-support-matrix).

The [`import_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.import_table) method of the [Python client](../../getting-started/pyclient-quickstart.md) replaces each field's metadata with its own:

- It sets `deephaven:type` from a fixed mapping for simple Arrow types.
- It leaves `deephaven:type` off for parameterized types, such as lists, maps, 128-bit and 256-bit decimals, and timestamps with a time zone. Those columns get their default Deephaven type.
- It never sets `deephaven:componentType`.
- It raises an error for any other Arrow type, including dictionary-encoded and run-end encoded columns.

To upload a type that `import_table` rejects, or to choose the column types yourself, upload with a Flight client such as [`pyarrow.flight`](https://arrow.apache.org/docs/python/flight.html), and put the type tags in each field's metadata.

Let's upload a table with `pyarrow.flight` and download it again. The schema asks Deephaven to store a 32-bit integer field as a `long` column. The upload's Flight descriptor, `scope` followed by a variable name, publishes the table to the server's [query scope](../query-scope.md) as `uploaded_table`. The download asks for that variable with the ticket `s/uploaded_table`. Run this code in a separate Python process that has `pyarrow` installed, not in the Deephaven server console. Replace `<your-pre-shared-key>` with the key the server prints to its log on startup:

```python skip-test
import pyarrow as pa
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:10000")

# Each call authenticates with the server's pre-shared key
options = flight.FlightCallOptions(
    headers=[
        (
            b"authorization",
            b"io.deephaven.authentication.psk.PskAuthenticationHandler <your-pre-shared-key>",
        )
    ]
)

# Ask Deephaven to store the 32-bit integer field as a long column
schema = pa.schema([pa.field("Count", pa.int32(), metadata={"deephaven:type": "long"})])
data = pa.table({"Count": pa.array([1, 2, None, 4], type=pa.int32())}, schema=schema)

# Upload the table to the query scope variable "uploaded_table"
descriptor = flight.FlightDescriptor.for_path("scope", "uploaded_table")
writer, reader = client.do_put(descriptor, schema, options)
writer.write_table(data)
writer.done_writing()
# Wait for the server to publish the table before you download it
reader.read()
writer.close()

# Download the table with the ticket "s/uploaded_table"
result = client.do_get(flight.Ticket(b"s/uploaded_table"), options).read_all()
print(result)
```

The downloaded column is still `int32` on the wire, even though the server stores it as a `long` column, because Deephaven keeps the uploaded schema (see [Round-tripping](#round-tripping)):

```text
pyarrow.Table
Count: int32
----
Count: [[1,2,null,4]]
```

Because this example sends the pre-shared key on every call, the server starts a new session for each call. A long-running client should instead reuse the session token the server returns, or use the [Python client](../../getting-started/pyclient-quickstart.md), which manages its session for you.

The Groovy guide's [Upload a table](/core/groovy/docs/how-to-guides/data-import-export/arrow-flight#upload-a-table) example shows a complete upload from an Arrow Java client. Its `Duration`-to-`long` mapping works only after you [register the custom mapping](/core/groovy/docs/how-to-guides/data-import-export/arrow-flight#register-the-custom-mapping) on the server.

### Export

You can specify the Flight schema, and so each column's wire type, to use when exporting a table to a Flight client. Use [`with_attributes`](../../reference/table-operations/create/withAttributes.md) to put an Arrow Java `org.apache.arrow.vector.types.pojo.Schema` object in the table's attributes under the key `BarrageSchema`. This overrides the schema Deephaven generates from the table's column types.

To change only some fields, start from the generated schema, which the Java method [`BarrageUtil.schemaFromTable`](https://docs.deephaven.io/core/javadoc/io/deephaven/extensions/barrage/util/BarrageUtil.html#schemaFromTable(io.deephaven.engine.table.Table)) returns. The generated schema may run-end encode or dictionary encode a column whose values repeat often. Check each field's type and dictionary before you replace its type.

Building the `Schema` object from Python means working with Arrow Java types through [`jpy`](../use-jpy.md). [Barrage schema annotation](./barrage-schema.md) outlines the steps, and the [Groovy Barrage schema annotation guide](/core/groovy/docs/how-to-guides/data-import-export/barrage-schema) has worked examples you can adapt.

### Round-tripping

When a client uploads a table with `DoPut`, Deephaven stores the Flight schema in the table's `BarrageSchema` attribute. This is convenient, because you can then download the table in exactly the same wire format.

Most table operations drop this attribute, even when they keep every column's type unchanged. Filters (such as [`where`](../../reference/table-operations/filter/where.md)), [`sort`](../../reference/table-operations/sort/sort.md), [`reverse`](../../reference/table-operations/sort/reverse.md), [`flatten`](../../reference/table-operations/select/flatten.md), [`first_by`](../../reference/table-operations/group-and-aggregate/firstBy.md), and [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md) keep it, and so do the constituent tables of a [`partition_by`](../../reference/table-operations/group-and-aggregate/partitionBy.md). To preserve the original wire types after any other operation, re-attach a schema with [`with_attributes`](../../reference/table-operations/create/withAttributes.md) as described in [Export](#export). The schema needs one field for each of the result table's columns, so after an operation that adds, drops, or renames columns, adjust the schema's fields to match.

## Turn off the default `toString` export

By default, when Deephaven has no writer for a column's type, it exports the column's values as UTF-8 strings, using each value's `toString`. This is not always desirable. Some types have no meaningful string form, and others can't be converted to a string without losing data. To turn off this behavior for every export from the server, call the Java method [`disableToStringUnknownTypes`](https://docs.deephaven.io/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkWriterFactory.html#disableToStringUnknownTypes()) through [`jpy`](../use-jpy.md):

```python reset
import jpy

_JDefaultChunkWriterFactory = jpy.get_type(
    "io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory"
)
_JDefaultChunkWriterFactory.INSTANCE.disableToStringUnknownTypes()
```

> [!IMPORTANT]
> After you call [`disableToStringUnknownTypes`](https://docs.deephaven.io/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkWriterFactory.html#disableToStringUnknownTypes()), exporting a column that has no explicit mapping throws a `StatusRuntimeException`. This applies to a `DoGet`, which is a client download, and to a `DoExchange`, which is a two-way stream such as a subscription. The remote client receives the exception as an `INVALID_ARGUMENT` gRPC error.
>
> To export such a column, register a writer for its type as described in [Custom mappings](#custom-mappings), next.

## Custom mappings

You implement custom type mappings in Java by registering readers and writers with the [`DefaultChunkReaderFactory`](https://docs.deephaven.io/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkReaderFactory.html) and [`DefaultChunkWriterFactory`](https://docs.deephaven.io/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkWriterFactory.html) classes. See the [Groovy Arrow Flight guide](/core/groovy/docs/how-to-guides/data-import-export/arrow-flight#custom-mappings) for a worked example that registers a mapping, uploads a table from an Arrow Java Flight client, and downloads it again.

## Related documentation

- [Barrage schema annotation](./barrage-schema.md)
- [What is Barrage?](../../conceptual/what-is-barrage.md)
- [Python client quickstart](../../getting-started/pyclient-quickstart.md)
- [Barrage Extensions Package Summary](https://docs.deephaven.io/core/javadoc/io/deephaven/extensions/barrage/package-summary.html)
- [Arrow Flight Package Summary](https://docs.deephaven.io/core/javadoc/org/apache/arrow/flight/package-summary.html)
