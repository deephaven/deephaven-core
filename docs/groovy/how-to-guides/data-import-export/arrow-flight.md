---
title: Arrow Flight and Deephaven
---

[Apache Arrow](https://arrow.apache.org/) is a high-performance, language-independent columnar memory format for flat and nested data. Deephaven's [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) integration lets you import and export data with remote Flight clients over gRPC. Deephaven's [Barrage](../../conceptual/what-is-barrage.md) protocol extends Flight. Clients that speak Barrage, such as Deephaven's own client APIs, can also subscribe to ticking tables, which update in real time. After an initial snapshot, a subscriber receives only what changes in each [update cycle](../../conceptual/table-update-model.md).

A table's _wire types_ are the Arrow types it uses on the network, and its _Flight schema_ lists them field by field. This guide shows how Arrow types map to Deephaven column types, how to choose the column types Deephaven creates when a Flight client uploads a table, how to choose the wire types Deephaven uses when it exports a table, and how to register your own type mappings.

## Arrow type support matrix

A coercion is a non-default Deephaven type that Deephaven can convert the Arrow type to on import, and convert back to the Arrow type on export. You request one on import with the [`deephaven:type`](#import) metadata tag, and on export by [attaching a Flight schema to the table](#export).

The following table lists each supported Arrow type, the Deephaven type it maps to by default, and its supported coercions.

| Arrow Type                            | Default Deephaven Type                                                                | Supported Coercions                                                                                                         | Notes                                                                                                                                                                                                                                                                                                                               |
| ------------------------------------- | ------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Bool`                                | `Boolean`                                                                             | `byte`, `boolean`                                                                                                           | Deephaven uses boxed `Boolean` so that cells can hold nulls. Without a [`deephaven:type`](#import) tag, or with a tag of `boolean`, a top-level `Bool` column is `Boolean`, even for a non-nullable field. Only the element field of a list keeps primitive `boolean` when it is non-nullable, which gives a `boolean[]` column.    |
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

Coercion from a larger integral type to a smaller one, or from an unsigned type to the signed type of the same width, follows Java's narrowing primitive conversion, which keeps only the low-order bits. Some values truncate to `null`, because Deephaven reserves one value of each primitive type as its null marker. For example, `0x2080` becomes `null` when coerced to a `byte`, because [`QueryConstants.NULL_BYTE`](/core/javadoc/io/deephaven/util/QueryConstants.html#NULL_BYTE) is `0x80`. Similarly, `-1` becomes `null` when coerced to a `char`, because [`QueryConstants.NULL_CHAR`](/core/javadoc/io/deephaven/util/QueryConstants.html#NULL_CHAR) is `0xFFFF`.

### Decimal coercion

Coercing a decimal value to a type with lower precision rounds or truncates it. For example, coercion to any integral type drops the fractional part of the decimal value.

## Unsupported Arrow types

Deephaven does not support the following Arrow types:

- Large types such as `LargeUtf8`, `LargeBinary`, `LargeList`, and `LargeListView`
- `Struct`
- `Utf8View` and `BinaryView`

Supporting the large types would require simulating 64-bit arrays.

## Set the Flight schema

A Flight schema controls types in both directions. On import, metadata tags on the uploaded schema choose the Deephaven column types. On export, a schema attached to the table chooses the wire types. Deephaven also keeps an uploaded schema, so you can download the table in the same wire format when its field names are valid column names. See [Round-tripping](#round-tripping).

### Import

`DoPut` is Flight's upload call. When a Flight client uploads data to Deephaven with `DoPut`, the client can specify the destination column types by adding metadata tags to the Flight schema's fields.

The metadata keys are as follows:

- `deephaven:type`: the canonical class name of the Deephaven type to use for the column. For a list field, this can be an array class, such as `int[]`, or a vector class, such as `io.deephaven.vector.LongVector`.
- `deephaven:componentType`: the canonical class name of the element type to use for the column. For an `ObjectVector` column, set it to choose the element class. Without it, Deephaven uses the default type of the list's element field. For an array column or a primitive vector column, Deephaven infers the element type from `deephaven:type`.

Any field without type tags gets the default Deephaven type for its Arrow wire type, as listed in the [Arrow type support matrix](#arrow-type-support-matrix).

The following schema asks Deephaven to store a 32-bit integer field as a `long` column:

```groovy skip-test
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema

FieldType fieldType = new FieldType(true, new ArrowType.Int(32, true), null, Map.of("deephaven:type", "long"))
Schema schema = new Schema(List.of(new Field("Count", fieldType, null)))
```

Because `Int(32, signed)` to `long` is a supported coercion, you don't need a custom reader. To upload this schema:

1. Connect as shown in [Prepare the remote client](#prepare-the-remote-client).
2. Copy the [Upload a table](#upload-a-table) code, and delete the lines that build `attrs`, `arrowType`, `fieldType`, and `schema`, so the code uses the `schema` above.
3. Import `org.apache.arrow.vector.IntVector` in place of `DurationVector`.
4. Change `DurationVector vector = (DurationVector) source.getVector(0)` to `IntVector vector = (IntVector) source.getVector(0)`.

### Export

To choose each column's wire type on export, attach a Flight schema to the table. The schema you attach overrides the default schema, which Deephaven generates from the table's column types.

- The schema is an Arrow Java `org.apache.arrow.vector.types.pojo.Schema`.
- Attach it with [`withAttributes`](../../reference/table-operations/select/withAttributes.md) under the key [`Table.BARRAGE_SCHEMA_ATTRIBUTE`](/core/javadoc/io/deephaven/engine/table/Table.html#BARRAGE_SCHEMA_ATTRIBUTE) (the attribute name `BarrageSchema`).

When you build the schema:

- Start from [`BarrageUtil.schemaFromTable`](/core/javadoc/io/deephaven/extensions/barrage/util/BarrageUtil.html#schemaFromTable(io.deephaven.engine.table.Table)), which returns the default schema, and change only the fields you need.
- Check each field's type and dictionary before you replace its type. The default schema may run-end encode a column that has runs of repeated values, or dictionary encode a `String` or other object column that has few distinct values. [Verify the encoding from a subscriber](./barrage-schema.md#verify-the-encoding-from-a-subscriber) shows how to check which encoding a client receives.

The following example fetches the default schema and changes the export wire type of column `D` from `Int(64, signed)` to `Timestamp(MILLISECOND)`. Deephaven's default `long` writer treats each value as nanoseconds and converts it to milliseconds on the wire. The column has no repeated values, so Deephaven doesn't encode its field, and the example can replace the field's type directly:

```groovy order=null
import io.deephaven.engine.table.Table
import io.deephaven.extensions.barrage.util.BarrageUtil
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema

t = emptyTable(32).update("D = ii * 1_000_000_000L")

Schema currSchema = BarrageUtil.schemaFromTable(t)
Field currField = currSchema.findField("D")
FieldType currFtype = currField.getFieldType()
Map<String, String> currMetadata = currField.getMetadata()
// currMetadata already sets deephaven:type to long, so a receiving Deephaven server creates a long column.
// To choose a different column type, copy the map first (it is immutable): new HashMap<>(currMetadata), then change deephaven:type.

ArrowType newType = new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)
FieldType newFtype = new FieldType(currFtype.isNullable(), newType, currFtype.getDictionary(), currMetadata)
List<Field> newFields = new ArrayList<>(currSchema.getFields())
newFields.set(newFields.indexOf(currField), new Field(currField.getName(), newFtype, currField.getChildren()))
Schema newSchema = new Schema(newFields, currSchema.getCustomMetadata())

// now set the table attribute to advertise how to write this on the wire
t = t.withAttributes(Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, newSchema))
```

A Flight client that downloads `t` receives column `D` as `Timestamp(MILLISECOND)`. To check, see [Verify the encoding from a subscriber](./barrage-schema.md#verify-the-encoding-from-a-subscriber). The web UI is also a Barrage client, but it reads `Timestamp` columns only in `NANOSECOND` units, so it can't display `t`.

For focused examples that use [`Table.BARRAGE_SCHEMA_ATTRIBUTE`](/core/javadoc/io/deephaven/engine/table/Table.html#BARRAGE_SCHEMA_ATTRIBUTE) for `Union`, `Map`, run-end encoded, and dictionary-encoded wire types, see [Barrage schema annotation](barrage-schema.md).

### Round-tripping

When a client uploads a table with `DoPut`, Deephaven stores the Flight schema as a table attribute under the key [`Table.BARRAGE_SCHEMA_ATTRIBUTE`](/core/javadoc/io/deephaven/engine/table/Table.html#BARRAGE_SCHEMA_ATTRIBUTE), which holds the name `BarrageSchema`. You can then download the table in exactly the same wire format, as long as every uploaded field name is already a valid, unique Deephaven column name. Deephaven renames other fields on import so that they become valid column names, but the stored schema keeps the original names, so a later download of the table fails.

Most table operations drop this attribute, even when they keep every column's type unchanged. The following operations keep it:

- [Filters](../use-filters.md), such as [`where`](../../reference/table-operations/filter/where.md), [`whereIn`](../../reference/table-operations/filter/where-in.md), and [`whereNotIn`](../../reference/table-operations/filter/where-not-in.md)
- [`sort`](../../reference/table-operations/sort/sort.md) and [`sortDescending`](../../reference/table-operations/sort/sort-descending.md)
- [`reverse`](../../reference/table-operations/sort/reverse.md)
- [`flatten`](../../reference/table-operations/create/flatten.md)
- [`firstBy`](../../reference/table-operations/group-and-aggregate/firstBy.md)
- [`lastBy`](../../reference/table-operations/group-and-aggregate/lastBy.md)
- [`partitionBy`](../../reference/table-operations/group-and-aggregate/partitionBy.md), for each of its constituent tables
- [`withAttributes`](../../reference/table-operations/select/withAttributes.md) and the other attribute methods, unless they remove it

[Row-position filters](../use-filters.md#filter-by-row-position), such as [`head`](../../reference/table-operations/filter/head.md) and [`tail`](../../reference/table-operations/filter/tail.md), drop it.

To preserve the original wire types after any other operation, re-attach a schema with [`withAttributes`](../../reference/table-operations/select/withAttributes.md) as shown in [Export](#export). The schema needs one field for each column of the result table, in the table's column order. The schema sets wire types, not which columns are exported, so a schema that leaves out or reorders columns can make the export fail or pair data with the wrong field. After an operation that adds, drops, or renames columns, adjust the schema's fields to match.

## Upload and download from a Java Flight client

This section shows how to connect to Deephaven from an Arrow Java Flight client, upload a table with `DoPut`, and download it with `DoGet`. Every step runs in the Flight client, such as a separate Java or Groovy process that has Arrow Flight on its classpath, not in the Deephaven server console.

The upload example sends Arrow `Duration` values and tags the field with [`deephaven:type`](#import) `long`. `Duration` to `long` isn't a built-in coercion, so register the reader and writer on the server, as shown in [Register the custom mapping](#register-the-custom-mapping), before you run it. To upload without a custom mapping, use the schema from [Import](#import) and make the changes listed there.

### Prepare the remote client

The client connects to the server and authenticates with the server's [pre-shared key](../authentication/auth-psk.md), which a default server prints to its log on startup. A server that uses [anonymous authentication](../authentication/auth-anon.md) accepts the `Anonymous` header shown in the comment.

```groovy skip-test
import org.apache.arrow.flight.CallHeaders
import org.apache.arrow.flight.FlightCallHeaders
import org.apache.arrow.flight.FlightClient
import org.apache.arrow.flight.HeaderCallOption
import org.apache.arrow.flight.Location
import org.apache.arrow.flight.client.ClientCookieMiddleware
import org.apache.arrow.memory.RootAllocator

allocator = new RootAllocator()
client = FlightClient.builder()
        .allocator(allocator)
        .location(Location.forGrpcInsecure("localhost", 10000))
        .intercept(new ClientCookieMiddleware.Factory())
        .build()

CallHeaders headers = new FlightCallHeaders()
headers.insert("Authorization", "io.deephaven.authentication.psk.PskAuthenticationHandler <your-pre-shared-key>")
// a server configured for anonymous authentication accepts this instead:
// headers.insert("Authorization", "Anonymous")
headers.insert("x-deephaven-auth-cookie-request", "true")
client.handshake(new HeaderCallOption(headers))
```

> [!WARNING]
> Long-running remote Deephaven clients must send heartbeats to the server to keep session state alive. Any Deephaven gRPC call that passes the current authentication cookie in its headers counts as a heartbeat.
>
> - Arrow Flight client middleware, such as the `ClientCookieMiddleware` in this example, attaches the cookie it last received to each call.
> - The server sends a refreshed cookie only in response to a call that includes the `x-deephaven-auth-cookie-request: true` header. This example sends that header only with the handshake.
> - A long-running client must also send that header on later calls. Otherwise its cookie expires even while it keeps making calls.
> - The middleware doesn't make calls on its own, so an otherwise idle client must make a call periodically.
>
> Use an official Deephaven client where you can, because it meets these server requirements for you.

### Upload a table

This step uploads millisecond `Duration` values to the [query scope](../query-scope.md) variable `uploadedTable`, and tags the field with [`deephaven:type`](#import) so that Deephaven stores the column as a `long`.

```groovy skip-test
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.flight.FlightDescriptor
import org.apache.arrow.flight.AsyncPutListener
import org.apache.arrow.vector.DurationVector
import org.apache.arrow.vector.VectorSchemaRoot

// Prepare the schema with appropriate metadata to specify the target column type for the Deephaven table
COLUMN_NAME = "Duration"
Map<String, String> attrs = new HashMap<>()
attrs.put("deephaven:type", "long")
boolean nullable = true
ArrowType arrowType = new ArrowType.Duration(TimeUnit.MILLISECOND)
FieldType fieldType = new FieldType(nullable, arrowType, null, attrs)
Schema schema = new Schema(Collections.singletonList(new Field(COLUMN_NAME, fieldType, null)))

// Upload data:
NUM_ROWS = 32
try (final VectorSchemaRoot source = VectorSchemaRoot.create(schema, allocator)) {
    source.allocateNew()
    source.setRowCount(NUM_ROWS)
    DurationVector vector = (DurationVector) source.getVector(0)

    rnd = new java.util.Random()
    for (int ii = 0; ii < NUM_ROWS; ++ii) {
        // random millisecond durations
        if (ii % 8 == 7) {
            vector.setNull(ii)
        } else {
            vector.set(ii, rnd.nextInt(1000))
        }
    }

    // upload the table into a QueryScope variable named "uploadedTable"
    FlightDescriptor descriptor = FlightDescriptor.path("scope", "uploadedTable")
    AsyncPutListener listener = new AsyncPutListener()
    FlightClient.ClientStreamListener putStream = client.startPut(descriptor, source, listener)
    putStream.putNext()
    putStream.completed()
    // ensure that the server finishes publishing the result table to the QueryScope
    listener.getResult()
}
```

### Download the table

This step downloads the table with the ticket `s/uploadedTable`. A ticket names an object on the server, and the `s/` prefix names a [query scope](../query-scope.md) variable.

You don't need to set [`Table.BARRAGE_SCHEMA_ATTRIBUTE`](/core/javadoc/io/deephaven/engine/table/Table.html#BARRAGE_SCHEMA_ATTRIBUTE) for this download. Deephaven keeps the uploaded schema on the table and uses it when you download that table, or a table derived from it by an operation that keeps the attribute. [Round-tripping](#round-tripping) lists those operations.

```groovy skip-test
import org.apache.arrow.flight.FlightStream
import org.apache.arrow.flight.Ticket
import java.nio.charset.StandardCharsets

Ticket ticket = new Ticket("s/uploadedTable".getBytes(StandardCharsets.UTF_8))
try (final FlightStream stream = client.getStream(ticket)) {
    while (stream.next()) {
        println(stream.getRoot().getVector(0))
    }
}
```

With the custom mapping registered, the custom writer converts the stored nanoseconds back to the field's time unit, so the client receives millisecond `Duration` values again. For the sample upload shown later in [Verify the upload on the server](#verify-the-upload-on-the-server), the output looks like this:

```text
[PT0.979S, PT0.384S, PT0.186S, PT0.531S, PT0.542S, PT0.44S, PT0.576S, null, PT0.683S, PT0.699S, ... PT0.868S, null, PT0.118S, PT0.003S, PT0.356S, PT0.907S, PT0.799S, PT0.248S, PT0.372S, null]
```

## Turn off the default `toString` export

A _writer_ converts a column type to an Arrow wire type on export. When Deephaven has no writer for a column's type, it sends each value's `toString` as UTF-8 text. That suits most types. It doesn't suit types with no meaningful string form, or types that lose data as text.

To turn off this fallback for every export from the server, call [`disableToStringUnknownTypes`](/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkWriterFactory.html#disableToStringUnknownTypes()):

```groovy reset
import io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory

DefaultChunkWriterFactory.INSTANCE.disableToStringUnknownTypes()
```

> [!IMPORTANT]
> After you call [`disableToStringUnknownTypes`](/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkWriterFactory.html#disableToStringUnknownTypes()), exporting a column that has no explicit mapping throws a `StatusRuntimeException`. This applies to both of the following calls:
>
> - `DoGet`, a client download
> - `DoExchange`, a two-way stream such as a subscription
>
> The remote client receives the exception as an `INVALID_ARGUMENT` gRPC error.
>
> To export such a column, register a writer for its type as described in the next section, [Custom mappings](#custom-mappings).

## Custom mappings

You can register new Arrow-to-Deephaven type mappings, or replace the reader (which converts an Arrow wire type to a column type on import) or writer for an existing pair. Deephaven uses an Arrow type's default Deephaven type when a field has no [`deephaven:type`](#import) tag. You can't change that default.

The [`DefaultChunkReaderFactory`](/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkReaderFactory.html) and [`DefaultChunkWriterFactory`](/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkWriterFactory.html) classes maintain the registry of import/export mappings. Their [source code](https://github.com/deephaven/deephaven-core/tree/main/extensions/barrage/src/main/java/io/deephaven/extensions/barrage/chunk) shows more examples of reusing Deephaven's internal helpers in your own readers and writers.

Let's add and use a custom mapping from an Arrow `Duration` to primitive `long` and back. The client steps use the code in [Upload and download from a Java Flight client](#upload-and-download-from-a-java-flight-client).

1. On the server, register the custom mapping with [`DefaultChunkReaderFactory`](/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkReaderFactory.html) and [`DefaultChunkWriterFactory`](/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkWriterFactory.html).
2. On the client, [prepare the remote client](#prepare-the-remote-client) and [upload a table](#upload-a-table) of `Duration` values to the [query scope](../query-scope.md) variable `uploadedTable`.
3. On the server, verify the uploaded table's column type and data.
4. On the client, [download the table](#download-the-table) and print the returned data.

> [!IMPORTANT]
> This example uses Groovy for brevity. For production, implement type mappings in Java and load them with [Application Mode](../application-mode.md).

### Register the custom mapping

This step runs on the Deephaven server. It registers a custom reader and writer for the Arrow `Duration` type. The reader converts incoming `Duration` values to a primitive `long` column in nanoseconds. The writer converts the nanoseconds back to the field's time unit on export.

```groovy reset
import java.time.Duration
import io.deephaven.util.QueryConstants
import io.deephaven.chunk.*
import io.deephaven.chunk.attributes.*
import io.deephaven.extensions.barrage.chunk.*
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType


long factorForTimeUnit(TimeUnit unit) {
    switch (unit) {
        case TimeUnit.NANOSECOND:
            return 1L
        case TimeUnit.MICROSECOND:
            return 1000L
        case TimeUnit.MILLISECOND:
            return 1000 * 1000L
        case TimeUnit.SECOND:
            return 1000 * 1000 * 1000L
        default:
            throw new RuntimeException("unknown time unit")
    }
}

DefaultChunkReaderFactory.INSTANCE.register(
        ArrowType.ArrowTypeID.Duration,
        long.class,
        (arrowType, typeInfo, options) -> {
            final long factor = factorForTimeUnit(((ArrowType.Duration) arrowType).getUnit());
            return new LongChunkReader(options) {
                @Override def WritableLongChunk<Values> readChunk(
                        Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
                        PrimitiveIterator.OfLong bufferInfoIter,
                        DataInput is,
                        WritableChunk<Values> outChunk,
                        final int outOffset,
                        final int totalRows) throws IOException {
                    final WritableLongChunk<Values> values =
                            super.readChunk(fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                    for (int ii = outOffset; ii < values.size(); ++ii) {
                        if (!values.isNull(ii)) {
                            values.set(ii, values.get(ii) * factor);
                        }
                    }
                    return values;
                }
            };
        }
)

DefaultChunkWriterFactory.INSTANCE.register(
        ArrowType.ArrowTypeID.Duration,
        long.class,
        typeInfo -> {
            final long factor = factorForTimeUnit(((ArrowType.Duration) typeInfo.arrowField().getType()).getUnit());
            return new LongChunkWriter<>((source) -> {
                final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(source.size());
                for (int ii = 0; ii < source.size(); ++ii) {
                    final long value = source.get(ii); // assume in nanoseconds
                    chunk.set(ii, (long)(value == QueryConstants.NULL_LONG ? QueryConstants.NULL_LONG : value / factor));
                }
                return chunk;
            }, LongChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
        }
)
```

### Verify the upload on the server

Run these snippets in the Deephaven server console to verify that the client uploaded the table correctly. First, check the column type:

```groovy skip-test
import io.deephaven.engine.util.TableTools

TableTools.show(uploadedTable.meta())
```

The console output, written out here as a table, shows that the destination column type is `long`:

| Name     | DataType | ColumnType | IsPartitioning |
| -------- | -------- | ---------- | -------------- |
| Duration | long     | Normal     | false          |

Next, print the table's data:

```groovy skip-test
import io.deephaven.engine.util.TableTools

// print the first ten rows of the table
TableTools.show(uploadedTable)
```

The console output, written out here as a table, shows values in nanoseconds. The client uploaded milliseconds, and the custom reader multiplied each value by the time-unit factor. Your values differ from these because the client uploads random durations:

| Duration  |
| --------- |
| 979000000 |
| 384000000 |
| 186000000 |
| 531000000 |
| 542000000 |
| 440000000 |
| 576000000 |
| (null)    |
| 683000000 |
| 699000000 |

Finally, in the Flight client, [download the table](#download-the-table). The custom writer converts the nanoseconds back to milliseconds, so the client prints `PT0.979S` for the first value above.

## Related documentation

- [What is Barrage?](../../conceptual/what-is-barrage.md)
- [Incremental update model](../../conceptual/table-update-model.md)
- [Barrage schema annotation](./barrage-schema.md)
- [Application Mode](../application-mode.md)
- [Pre-shared key authentication](../authentication/auth-psk.md)
- [Anonymous authentication](../authentication/auth-anon.md)
- [Filter table data](../use-filters.md)
- [Query scope](../query-scope.md)
- [`emptyTable`](../../reference/table-operations/create/emptyTable.md)
- [`firstBy`](../../reference/table-operations/group-and-aggregate/firstBy.md)
- [`flatten`](../../reference/table-operations/create/flatten.md)
- [`head`](../../reference/table-operations/filter/head.md)
- [`lastBy`](../../reference/table-operations/group-and-aggregate/lastBy.md)
- [`meta`](../../reference/table-operations/metadata/meta.md)
- [`partitionBy`](../../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`reverse`](../../reference/table-operations/sort/reverse.md)
- [`sort`](../../reference/table-operations/sort/sort.md)
- [`sortDescending`](../../reference/table-operations/sort/sort-descending.md)
- [`tail`](../../reference/table-operations/filter/tail.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`where`](../../reference/table-operations/filter/where.md)
- [`whereIn`](../../reference/table-operations/filter/where-in.md)
- [`whereNotIn`](../../reference/table-operations/filter/where-not-in.md)
- [`withAttributes`](../../reference/table-operations/select/withAttributes.md)
- [`BarrageUtil` Javadoc](/core/javadoc/io/deephaven/extensions/barrage/util/BarrageUtil.html)
- [`DefaultChunkReaderFactory` Javadoc](/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkReaderFactory.html)
- [`DefaultChunkWriterFactory` Javadoc](/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkWriterFactory.html)
- [`QueryConstants` Javadoc](/core/javadoc/io/deephaven/util/QueryConstants.html)
- [`Table` Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
- [Arrow Flight Java API (`org.apache.arrow.flight`)](https://arrow.apache.org/java/current/reference/org.apache.arrow.flight.core/org/apache/arrow/flight/package-summary.html)
