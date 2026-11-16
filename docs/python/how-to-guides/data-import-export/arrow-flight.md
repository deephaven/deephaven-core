---
title: Arrow Flight and Deephaven
---

[Apache Arrow](https://arrow.apache.org/) is a high-performance, language-independent columnar memory format for flat and nested data. Deephaven's [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) integration lets you import and export data with remote Flight clients over gRPC. Deephaven's [Barrage](../../conceptual/what-is-barrage.md) protocol extends Flight. Clients that speak Barrage, such as Deephaven's own client APIs, can also subscribe to ticking tables, which update in real time. After an initial snapshot, a subscriber receives only what changes in each [update cycle](../../conceptual/table-update-model.md).

A table's _wire types_ are the Arrow types it uses on the network, and its _Flight schema_ lists them field by field. This guide shows how Arrow types map to Deephaven column types, how to choose the column types Deephaven creates when a Flight client uploads a table, how to choose the wire types Deephaven uses when it exports a table, and where to learn how to register your own type mappings.

## Arrow type support matrix

Deephaven column types are Java types. For example, `long` is a Java primitive, and `Instant` is the Java class `java.time.Instant`.

A coercion is a non-default Deephaven type that Deephaven can convert the Arrow type to on import, and convert back to the Arrow type on export. You request one on import with the [`deephaven:type`](#import) metadata tag, and on export by [attaching a Flight schema to the table](#export).

The following table lists each supported Arrow type, the Deephaven type it maps to by default, and its supported coercions.

| Arrow Type                            | Default Deephaven Type                                                                | Supported Coercions                                                                                                         | Notes                                                                                                                                                                                                                                                                                                                               |
| ------------------------------------- | ------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Bool`                                | `Boolean`                                                                             | `byte`, `boolean`                                                                                                           | Deephaven uses boxed `Boolean` so that cells can hold nulls. A top-level `Bool` column is always `Boolean`, even for a non-nullable field or a [`deephaven:type`](#import) of `boolean`. Only the element field of a list keeps primitive `boolean` when it is non-nullable, which gives a `boolean[]` column.                      |
| `Int(8, signed)`                      | `byte`                                                                                | `char`, `short`, `int`, `long`, `float`, `double`, `BigInteger`, `BigDecimal`                                               | See notes on [integral coercion](#integral-coercion).                                                                                                                                                                                                                                                                               |
| `Int(8, unsigned)`                    | `short`                                                                               | `byte`, `char`, `int`, `long`, `float`, `double`, `BigInteger`, `BigDecimal`                                                | See notes on [integral coercion](#integral-coercion).                                                                                                                                                                                                                                                                               |
| `Int(16, signed)`                     | `short`                                                                               | `byte`, `char`, `int`, `long`, `float`, `double`, `BigInteger`, `BigDecimal`                                                | See notes on [integral coercion](#integral-coercion).                                                                                                                                                                                                                                                                               |
| `Int(16, unsigned)`                   | `char`                                                                                | `byte`, `short`, `int`, `long`, `float`, `double`, `BigInteger`, `BigDecimal`                                               | See notes on [integral coercion](#integral-coercion).                                                                                                                                                                                                                                                                               |
| `Int(32, signed)`                     | `int`                                                                                 | `byte`, `char`, `short`, `long`, `float`, `double`, `BigInteger`, `BigDecimal`                                              | See notes on [integral coercion](#integral-coercion).                                                                                                                                                                                                                                                                               |
| `Int(32, unsigned)`                   | `long`                                                                                | `byte`, `char`, `short`, `int`, `float`, `double`, `BigInteger`, `BigDecimal`                                               | See notes on [integral coercion](#integral-coercion).                                                                                                                                                                                                                                                                               |
| `Int(64, signed)`                     | `long`                                                                                | `byte`, `char`, `short`, `int`, `float`, `double`, `BigInteger`, `BigDecimal`                                               | See notes on [integral coercion](#integral-coercion).                                                                                                                                                                                                                                                                               |
| `Int(64, unsigned)`                   | `BigInteger`                                                                          | `byte`, `char`, `short`, `int`, `long`, `float`, `double`, `BigDecimal`                                                     | See notes on [integral coercion](#integral-coercion).                                                                                                                                                                                                                                                                               |
| `Decimal`                             | `BigDecimal`                                                                          | `byte`, `char`, `short`, `int`, `long`, `float`, `double`, `BigInteger`                                                     | See notes on [decimal coercion](#decimal-coercion).                                                                                                                                                                                                                                                                                 |
| `FloatingPoint(precision)`            | `float` (for `HALF`/`SINGLE`) or `double` (for `DOUBLE`)                              | `byte`, `char`, `short`, `int`, `long`, `BigDecimal`, `BigInteger`, and whichever of `float` or `double` is not the default | See notes on [decimal coercion](#decimal-coercion).                                                                                                                                                                                                                                                                                 |
| `Time(timeUnit, bitWidth)`            | `LocalTime`                                                                           | —                                                                                                                           | Time-of-day values. The bit width, 32 or 64, depends on the time unit.                                                                                                                                                                                                                                                              |
| `Timestamp(timeUnit, [timeZone])`     | `Instant`, or `ZonedDateTime` when the field has a non-UTC time zone                  | `long`, `LocalDateTime`, and whichever of `Instant` or `ZonedDateTime` is not the default                                   | A `long` column mapped to `Timestamp` holds nanoseconds. On import, Deephaven converts each value from the field's `TimeUnit` to nanoseconds. On export, it converts each value back to the field's `TimeUnit`.                                                                                                                     |
| `Date(dateUnit)`                      | `LocalDate`                                                                           | —                                                                                                                           |                                                                                                                                                                                                                                                                                                                                     |
| `Duration(timeUnit)`                  | `Duration`                                                                            | —                                                                                                                           |                                                                                                                                                                                                                                                                                                                                     |
| `Interval(intervalUnit)`              | `Period` (`YEAR_MONTH`), `Duration` (`DAY_TIME`), `PeriodDuration` (`MONTH_DAY_NANO`) | `Period` (`DAY_TIME` or `MONTH_DAY_NANO`), `PeriodDuration` (`YEAR_MONTH` or `DAY_TIME`)                                    | `PeriodDuration` is Arrow Java's class. Deephaven uses it to match the Arrow Java implementation. Coercing `DAY_TIME` or `MONTH_DAY_NANO` to `Period` drops the time of day.                                                                                                                                                        |
| `Utf8`                                | `String`                                                                              | —                                                                                                                           | Text encoded as UTF-8.                                                                                                                                                                                                                                                                                                              |
| `Binary`                              | `byte[]`                                                                              | `ByteVector`, `ByteBuffer`, `String`, `BigDecimal`, `BigInteger`, Arrow Java's `Schema`                                     | [Custom mappings](#custom-mappings) are supported. `String` is sent as UTF-8 bytes, `BigInteger` as the bytes of `BigInteger.toByteArray`, `BigDecimal` as a 4-byte little-endian scale followed by the unscaled value's `toByteArray` bytes, after removing trailing zeros, and `Schema` as a serialized Arrow IPC schema message. |
| `FixedSizeBinary(fixedLength)`        | `byte[]`                                                                              | `ByteVector`, `ByteBuffer`                                                                                                  | [Custom mappings](#custom-mappings) are supported.                                                                                                                                                                                                                                                                                  |
| `SparseUnion`/`DenseUnion`            | `Object`                                                                              | —                                                                                                                           | Union types (dense or sparse) map to a generic `Object` column type.                                                                                                                                                                                                                                                                |
| `Null`                                | `Object`                                                                              | —                                                                                                                           | Best when coupled with a `Union` or when [specifying the Deephaven type to use](#import).                                                                                                                                                                                                                                           |
| `Map`                                 | `Map`                                                                                 | —                                                                                                                           | Deephaven collects each cell into a `LinkedHashMap`. You can't choose a different collector. A Deephaven column doesn't record the key or value types of its maps, so Deephaven exports a `Map` column as strings unless you [attach a Flight schema for the export](#export).                                                      |
| `List` / `ListView` / `FixedSizeList` | `T[]` — a native Java array of the inner field's default type                         | Other array types, such as `int[]`, and `Vector` types, such as `LongVector` and `ObjectVector`                             | Deephaven truncates or null-pads each value to fit a fixed-size list wire format. To choose the array or vector type on import, see [Import](#import).                                                                                                                                                                              |
| `RunEndEncoded`                       | Default type of the values child                                                      | —                                                                                                                           | The column type comes from the values child. The `run_ends` child is only an index.                                                                                                                                                                                                                                                 |
| Dictionary-encoded fields             | Default type of the dictionary value type                                             | —                                                                                                                           | Applies to any field that carries a `DictionaryEncoding`.                                                                                                                                                                                                                                                                           |

### Integral coercion

Coercion from a larger integral type to a smaller one, or from an unsigned type to the signed type of the same width, follows Java's narrowing primitive conversion, which keeps only the low-order bits. Some values truncate to `null`, because Deephaven reserves one value of each primitive type as its null marker. For example, `0x2080` becomes `null` when coerced to a `byte`, because [`NULL_BYTE`](/core/pydoc/code/deephaven.constants.html#deephaven.constants.NULL_BYTE) is `0x80`. Similarly, `-1` becomes `null` when coerced to a `char`, because [`NULL_CHAR`](/core/pydoc/code/deephaven.constants.html#deephaven.constants.NULL_CHAR) is `0xFFFF`.

### Decimal coercion

Coercing a decimal value to a type with lower precision rounds or truncates it. For example, coercion to any integral type drops the fractional part of the decimal value.

## Unsupported Arrow types

Deephaven does not support the following Arrow types:

- Large types such as `LargeUtf8`, `LargeBinary`, `LargeList`, and `LargeListView`
- `Struct`
- `Utf8View` and `BinaryView`

Supporting the large types would require simulating 64-bit arrays.

## Set the Flight schema

A Flight schema controls types in both directions. On import, metadata tags on the uploaded schema choose the Deephaven column types. On export, a schema attached to the table chooses the wire types. Deephaven also keeps an uploaded schema, so you can download the table in the same wire format. See [Round-tripping](#round-tripping).

### Import

`DoPut` is Flight's upload call. When a Flight client uploads data to Deephaven with `DoPut`, the client can specify the destination column types by adding metadata tags to the Flight schema's fields.

The metadata keys are as follows:

- `deephaven:type`: the canonical class name of the Deephaven type to use for the column. For a list field, this can be an array class, such as `int[]`, or a vector class, such as `io.deephaven.vector.LongVector`.
- `deephaven:componentType`: the canonical class name of the element type to use for the column. For an `ObjectVector` column, set it to choose the element class. Without it, Deephaven uses the default type of the list's element field. For an array column or a primitive vector column, Deephaven infers the element type from `deephaven:type`.

Any field without type tags gets the default Deephaven type for its Arrow wire type, as listed in the [Arrow type support matrix](#arrow-type-support-matrix).

The [`import_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.import_table) method of the [Python client](../../getting-started/pyclient-quickstart.md) discards each field's metadata and writes new metadata:

- It sets `deephaven:type` from a fixed mapping for simple Arrow types.
- It leaves `deephaven:type` off for parameterized types, such as lists, maps, 128-bit and 256-bit decimals, and timestamps with a time zone. Those columns get their default Deephaven type.
- It never sets `deephaven:componentType`.
- It maps a dictionary-encoded or run-end encoded column by the type of its values. The column arrives as a plain column of that type.
- It raises an error for a list, map, or union whose element type it doesn't map, and for any other Arrow type.

To upload a type that [`import_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.import_table) rejects, or to choose the column types yourself, upload with a Flight client such as [`pyarrow.flight`](https://arrow.apache.org/docs/python/flight.html), and put the type tags in each field's metadata.

Let's upload a table with `pyarrow.flight` and download it again:

- The schema asks Deephaven to store a 32-bit integer field as a `long` column.
- The upload's Flight descriptor is `scope` followed by a variable name. It publishes the table to the server's [query scope](../query-scope.md) as `uploaded_table`.
- The download asks for that variable with the ticket `s/uploaded_table`. A ticket names an object on the server, and the `s/` prefix names a [query scope](../query-scope.md) variable.

Before you run the example:

- Run it in a separate Python process that has `pyarrow` installed, not in the Deephaven server console.
- Replace `<your-pre-shared-key>` with the key the server prints to its log on startup. See [pre-shared key authentication](../authentication/auth-psk.md).

Because the example sends the key on every call, the server starts a new session for each call. A long-running client should reuse the session token (the `Bearer` value) that the server returns in the `authorization` response header, or use the [Python client](../../getting-started/pyclient-quickstart.md), which manages its session for you.

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
# Read the server's acknowledgment of the uploaded batch
reader.read()
# Closing waits for the server to finish the upload and publish the table
writer.close()

# Download the table with the ticket "s/uploaded_table"
result = client.do_get(flight.Ticket(b"s/uploaded_table"), options).read_all()
print(result)
```

The server stores the column as a `long`, but the downloaded column is still `int32` on the wire. Deephaven keeps the uploaded schema and uses it for the download, as [Round-tripping](#round-tripping) explains. The output is:

```text
pyarrow.Table
Count: int32
----
Count: [[1,2,null,4]]
```

### Export

To choose each column's wire type on export, attach a Flight schema to the table. The schema you attach overrides the default schema, which Deephaven generates from the table's column types.

- The schema is an Arrow Java `org.apache.arrow.vector.types.pojo.Schema`, built through [`jpy`](../use-jpy.md).
- Attach it with [`with_attributes`](../../reference/table-operations/create/withAttributes.md) under the key `BarrageSchema` ([`Table.BARRAGE_SCHEMA_ATTRIBUTE`](/core/javadoc/io/deephaven/engine/table/Table.html#BARRAGE_SCHEMA_ATTRIBUTE)).

When you build the schema:

- Start from [`BarrageUtil.schemaFromTable`](/core/javadoc/io/deephaven/extensions/barrage/util/BarrageUtil.html#schemaFromTable(io.deephaven.engine.table.Table)), a Java method that returns the default schema, and change only the fields you need.
- Check each field's type and dictionary before you replace its type. The default schema may run-end encode a column that has runs of repeated values, or dictionary encode a `String` or other object column that has few distinct values. [Verify the encoding from a subscriber](./barrage-schema.md#verify-the-encoding-from-a-subscriber) shows how to check which encoding a client receives.

The following example changes only the wire type of column `D`, from `Int(64, signed)` to `Timestamp(MILLISECOND)`. Deephaven's default `long` writer treats each value as nanoseconds and converts it to milliseconds on the wire. The column has no repeated values, so Deephaven doesn't encode its field. The example builds the new `FieldType` from the default field's nullability, dictionary, and metadata:

```python order=null
from deephaven import empty_table
import jpy

BarrageUtil = jpy.get_type("io.deephaven.extensions.barrage.util.BarrageUtil")
JTable = jpy.get_type("io.deephaven.engine.table.Table")
ArrayList = jpy.get_type("java.util.ArrayList")
Field = jpy.get_type("org.apache.arrow.vector.types.pojo.Field")
FieldType = jpy.get_type("org.apache.arrow.vector.types.pojo.FieldType")
Schema = jpy.get_type("org.apache.arrow.vector.types.pojo.Schema")
TimeUnit = jpy.get_type("org.apache.arrow.vector.types.TimeUnit")
Timestamp = jpy.get_type("org.apache.arrow.vector.types.pojo.ArrowType$Timestamp")

t = empty_table(32).update("D = ii * 1_000_000_000L")

curr_schema = BarrageUtil.schemaFromTable(t.j_table)
curr_field = curr_schema.findField("D")
curr_ftype = curr_field.getFieldType()
new_ftype = FieldType(
    curr_ftype.isNullable(),
    Timestamp(TimeUnit.MILLISECOND, None),
    curr_ftype.getDictionary(),
    curr_field.getMetadata(),
)
fields = ArrayList(curr_schema.getFields())
fields.set(fields.indexOf(curr_field), Field("D", new_ftype, curr_field.getChildren()))
new_schema = Schema(fields, curr_schema.getCustomMetadata())

t = t.with_attributes({JTable.BARRAGE_SCHEMA_ATTRIBUTE: new_schema})
```

A Flight client that downloads `t` receives column `D` as `Timestamp(MILLISECOND)`. To check, see [Verify the encoding from a subscriber](./barrage-schema.md#verify-the-encoding-from-a-subscriber). The web UI is also a Barrage client, but it reads `Timestamp` columns only in `NANOSECOND` units, so it can't display `t`.

For more examples (`Union`, `Map`, run-end encoded, and dictionary-encoded wire types), see [Barrage schema annotation](./barrage-schema.md), which starts with the [steps to annotate a schema](./barrage-schema.md#steps-to-annotate-a-schema) and how to [get the Java classes](./barrage-schema.md#get-the-java-classes).

### Round-tripping

When a client uploads a table with `DoPut`, Deephaven stores the Flight schema in the table's `BarrageSchema` attribute. You can then download the table in exactly the same wire format.

Most table operations drop this attribute, even when they keep every column's type unchanged. The following operations keep it:

- [Filters](../use-filters.md), such as [`where`](../../reference/table-operations/filter/where.md), [`where_in`](../../reference/table-operations/filter/where-in.md), and [`where_not_in`](../../reference/table-operations/filter/where-not-in.md)
- [`sort`](../../reference/table-operations/sort/sort.md) and [`sort_descending`](../../reference/table-operations/sort/sort-descending.md)
- [`reverse`](../../reference/table-operations/sort/reverse.md)
- [`flatten`](../../reference/table-operations/select/flatten.md)
- [`first_by`](../../reference/table-operations/group-and-aggregate/firstBy.md)
- [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md)
- [`partition_by`](../../reference/table-operations/group-and-aggregate/partitionBy.md), for each of its constituent tables
- [`with_attributes`](../../reference/table-operations/create/withAttributes.md) and the other attribute methods, unless they remove it

[Row-position filters](../use-filters.md#filter-by-row-position), such as [`head`](../../reference/table-operations/filter/head.md) and [`tail`](../../reference/table-operations/filter/tail.md), drop it.

To preserve the original wire types after any other operation, re-attach a schema with [`with_attributes`](../../reference/table-operations/create/withAttributes.md) as described in [Export](#export). Deephaven exports only the columns that the schema names, so the schema needs one field for each column of the result table. After an operation that adds, drops, or renames columns, adjust the schema's fields to match.

## Turn off the default `toString` export

A _writer_ converts a column type to an Arrow wire type on export. When Deephaven has no writer for a column's type, it sends each value's `toString` as UTF-8 text. That suits most types. It doesn't suit types with no meaningful string form, or types that lose data as text.

To turn off this fallback for every export from the server, call the Java method [`disableToStringUnknownTypes`](/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkWriterFactory.html#disableToStringUnknownTypes()) through [`jpy`](../use-jpy.md):

```python reset
import jpy

_JDefaultChunkWriterFactory = jpy.get_type(
    "io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory"
)
_JDefaultChunkWriterFactory.INSTANCE.disableToStringUnknownTypes()
```

> [!IMPORTANT]
> After you call [`disableToStringUnknownTypes`](/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkWriterFactory.html#disableToStringUnknownTypes()), exporting a column that has no explicit mapping throws a `StatusRuntimeException`. This applies to both of the following calls:
>
> - `DoGet`, a client download
> - `DoExchange`, a two-way stream such as a subscription
>
> The remote client receives the exception as an `INVALID_ARGUMENT` gRPC error.
>
> To export such a column, register a writer for its type. [Custom mappings](#custom-mappings) links to a worked example.

## Custom mappings

You implement custom type mappings in Java. Register a reader, which converts an Arrow wire type to a column type on import, with [`DefaultChunkReaderFactory`](/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkReaderFactory.html), and a writer, which converts a column type to an Arrow wire type on export, with [`DefaultChunkWriterFactory`](/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkWriterFactory.html). See the [Groovy Arrow Flight guide](/core/groovy/docs/how-to-guides/data-import-export/arrow-flight#custom-mappings) for a worked example that registers a mapping, uploads a table from an Arrow Java Flight client, and downloads it again.

## Related documentation

- [What is Barrage?](../../conceptual/what-is-barrage.md)
- [Incremental update model](../../conceptual/table-update-model.md)
- [Barrage schema annotation](./barrage-schema.md)
- [Groovy Arrow Flight guide](/core/groovy/docs/how-to-guides/data-import-export/arrow-flight)
- [Groovy Barrage schema annotation guide](/core/groovy/docs/how-to-guides/data-import-export/barrage-schema)
- [Python client quickstart](../../getting-started/pyclient-quickstart.md)
- [Pre-shared key authentication](../authentication/auth-psk.md)
- [Anonymous authentication](../authentication/auth-anon.md)
- [Filter table data](../use-filters.md)
- [Query scope](../query-scope.md)
- [Use jpy](../use-jpy.md)
- [`empty_table`](../../reference/table-operations/create/emptyTable.md)
- [`first_by`](../../reference/table-operations/group-and-aggregate/firstBy.md)
- [`flatten`](../../reference/table-operations/select/flatten.md)
- [`head`](../../reference/table-operations/filter/head.md)
- [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md)
- [`partition_by`](../../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`reverse`](../../reference/table-operations/sort/reverse.md)
- [`sort`](../../reference/table-operations/sort/sort.md)
- [`sort_descending`](../../reference/table-operations/sort/sort-descending.md)
- [`tail`](../../reference/table-operations/filter/tail.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`where`](../../reference/table-operations/filter/where.md)
- [`where_in`](../../reference/table-operations/filter/where-in.md)
- [`where_not_in`](../../reference/table-operations/filter/where-not-in.md)
- [`with_attributes`](../../reference/table-operations/create/withAttributes.md)
- [`deephaven.constants` Pydoc](/core/pydoc/code/deephaven.constants.html)
- [`import_table` Pydoc](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.import_table)
- [`BarrageUtil` Javadoc](/core/javadoc/io/deephaven/extensions/barrage/util/BarrageUtil.html)
- [`DefaultChunkReaderFactory` Javadoc](/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkReaderFactory.html)
- [`DefaultChunkWriterFactory` Javadoc](/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkWriterFactory.html)
- [`Table` Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
