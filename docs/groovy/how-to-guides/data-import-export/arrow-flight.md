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
| `Binary`                              | `byte[]`                                                                        | `ByteVector`, `ByteBuffer`                                                     | [Custom mappings](#custom-mappings) are supported. Deephaven has custom encodings for BigDecimal, BigInteger, and Arrow Flight's Schema POJO.                                                |
| `FixedSizeBinary(fixedLength)`        | `byte[]`                                                                        | `ByteVector, ByteBuffer`                                                       | [Custom mappings](#custom-mappings) are supported.                                                                                                                                           |
| `SparseUnion`/`DenseUnion`            | `Object`                                                                        | —                                                                              | Union types (dense or sparse) map to a generic `Object` column type.                                                                                                                         |
| `Null`                                | `Object`                                                                        | —                                                                              | Best when coupled with a Union or when [specifying the Deephaven type to use](#set-the-flight-schema).                                                                                       |
| `Map`                                 | `LinkedHashMap`                                                                 | —                                                                              | Map types require that the [Arrow schema to be specified](#set-the-flight-schema). Cells are accumulated into `LinkedHashMap`s; it's not possible to pick another collector at this time.    |
| `List` / `ListView` / `FixedSizeList` | `T[]` - a native Java array of the inner field                                  | Supports conversion to Deephaven internal `Vector` types such as `LongVector`. | Note that the `deephaven:componentType` metadata key must be set to the inner type. Fixed-size list wire formats will be truncated and/or null-padded to match the data as best as possible. |

### Integral Coercion

Integral coercion from a larger type to a smaller type follows typical truncation rules, however, some values will be "truncated" to `null` values. For example `0x2080` will be truncated to `null` when coerced to a `byte` value as `QueryConstants.NULL_BYTE` is `0x80`. Similarly, any `-1` will be coerced to `null` when coerced to a `char` as `QueryConstants.NULL_CHAR` is `0xFFFF`.

### Decimal Coercion

Decimal coercion from a type with higher precision to a type with lower precision may result in rounding or truncation. For example, coercion to any integral type will lose the fractional part of the decimal value.

## Disable toString by Default

By default, unsupported types will be transformed into UTF-8 strings. This is not always desirable, especially for types that have no meaningful, or no lossless, toString representation. This feature can be disabled:

```groovy
import io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory

DefaultChunkWriterFactory.INSTANCE.disableToStringUnknownTypes()
```

> [!IMPORTANT]
> After `disableToStringUnknownTypes` is called, serializing columns that do not have an explicit mapping will cause DoGet and DoExchange to throw a StatusRuntimeException. This is propagated as an `INVALID_ARGUMENT` gRPC error to the remote client.

## Set the Flight Schema

### Import

When importing data into Deephaven using a Flight client and a DoPut call, the client can specify the destination column types by adding metadata tags to the Flight schema's fields.

The metadata keys are as follows:

- `deephaven:type` - The canonical class name of the Deephaven type to use for the column.
- `deephaven:componentType` - The canonical class name of the component type to use for the column. This is used for array types.

Any field/column that does not have type tags will use the default Deephaven type for the Arrow wire type.

See [uploading a table](#upload-a-table) for an example of setting the schema metadata from an Arrow-Java Flight client.

### Round-Tripping

Any table uploaded to Deephaven via a DoPut places the Flight Schema in a table attribute key of `BarrageSchema` (aka `io.deephaven.engine.Table.BARRAGE_SCHEMA_ATTRIBUTE`). This is convenient as it allows you to upload a table and then download the table using exactly the same wire format.

However, any table operations performed on the uploaded table will drop the schema metadata, even for column types that are completely preserved. Be sure to attach the modified schema if you want to preserve the original wire types.

### Exporting

You can specify the wire-type schema to use when exporting a table to a Flight client. Simply place the Schema POJO in the table's attributes with the key `BarrageSchema` (aka `io.deephaven.engine.Table.BARRAGE_SCHEMA_ATTRIBUTE`). This will override the default schema that would be generated from the table's column types.

Consider using [`BarrageUtil#schemaFromTable`](https://docs.deephaven.io/core/javadoc/io/deephaven/extensions/barrage/util/BarrageUtil.html#schemaFromTable(io.deephaven.engine.table.Table)) to generate the default schema and then modify it as needed.

Here is an example that fetches the default schema and then changes the wire type from `Int(64, signed)` to `Duration(ms)` for the column (for export):

```groovy
import io.deephaven.engine.table.Table
import io.deephaven.extensions.barrage.util.BarrageUtil
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema

Table t = emptyTable(32).update("D = ii * 1_000_000_000L")

Schema curr_schema = BarrageUtil.schemaFromTable(t)
Field curr_field = curr_schema.getFields().get(0)
FieldType curr_ftype = curr_field.getFieldType()
Map<String, String> curr_metadata = curr_field.getMetadata()
// note that we could replace the destination metadata deephaven type, too, if the remote client is a Deephaven worker

ArrowType new_type = new ArrowType.Duration(TimeUnit.MILLISECOND)
FieldType new_ftype = new FieldType(curr_ftype.isNullable(), new_type, curr_ftype.getDictionary(), curr_metadata);
List<Field> new_fields = List.of(new Field(curr_field.getName(), new_ftype, curr_field.getChildren()))
Schema new_schema = new Schema(new_fields, curr_schema.getCustomMetadata())

// now set the table attribute to advertise how to write this on the wire
t = t.withAttributes(Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, new_schema))
```

## Custom Mappings

Deephaven allows extending the set of supported type mappings. We currently do not support allowing the user to change the default mappings.

The [`DefaultChunkReaderFactory`](https://docs.deephaven.io/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkReaderFactory.html) and [`DefaultChunkWriterFactory`](https://docs.deephaven.io/core/javadoc/io/deephaven/extensions/barrage/chunk/DefaultChunkWriterFactory.html) classes maintain the registry of import/export mappings. Take a look at their source code for more examples and ideas on how to reuse Deephaven's internal helpers for your own custom serializers and deserializers.

Let's go through a full example of how to add and use a custom mapping from an Arrow `Duration` to primitive `long` and back.

1. Register the custom mapping with the `DefaultChunkReaderFactory` and `DefaultChunkWriterFactory`.
2. Prepare a remote client to connect to the Deephaven server.
3. Upload a table to the QueryScope variable `uploaded_table`.
4. Download the table from the server and print the returned data.

> [!IMPORTANT]
> While this example is provided as Groovy, it is a best practice to implement custom type mappings from Java within [Application Mode](../application-mode.md).

### Register the Custom Mapping

```groovy
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
            }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
        }
)
```

### Prepare the Remote Client

```groovy skip-test
import org.apache.arrow.flight.CallHeaders
import org.apache.arrow.flight.FlightCallHeaders
import org.apache.arrow.flight.FlightClient
import org.apache.arrow.flight.HeaderCallOption
import org.apache.arrow.flight.Location
import org.apache.arrow.flight.client.ClientCookieMiddleware

allocator = new RootAllocator()
client = FlightClient.builder()
        .allocator(allocator)
        .location(Location.forGrpcInsecure("localhost", 10000))
        .intercept(new ClientCookieMiddleware.Factory())
        .build();
{
    CallHeaders headers = new FlightCallHeaders()
    headers.insert("Authorization", "Anonymous")
    // pre-shared key authentication would look like this:
    // headers.insert("Authorization", "io.deephaven.authentication.psk.PskAuthenticationHandler $PRE_SHARED_KEY_VALUE$")
    headers.insert("x-deephaven-auth-cookie-request", "true")
    client.handshake(new HeaderCallOption(headers))
}
```

> [!WARNING]
> Long running remote Deephaven clients must heartbeat the server to keep session state alive. This can be done automatically using additional Arrow Flight Middleware. A heartbeat can be performed on any Deephaven gRPC call as long as the current authentication cookie is passed in the headers.
>
> It is highly recommended to use an official Deephaven client as often as possible to ensure that these details are implemented according to the server's requirements.

### Upload a table

```groovy skip-test
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema

// Prepare the schema with appropriate metadata to specify the target column type for the Deephaven table
COLUMN_NAME = "duration"
Map<String, String> attrs = new HashMap<>();
attrs.put("deephaven:type", "long");
// if this was an array/vector type, we would set the componentType, too:
// attrs.put("deephaven:componentType", "canonical.class.path.to.type");
boolean nullable = true
ArrowType arrowType = new ArrowType.Duration(TimeUnit.MILLISECOND)
FieldType fieldType = new FieldType(nullable, arrowType, null, attrs);
Schema schema = new Schema(Collections.singletonList(new Field(COLUMN_NAME, fieldType, null)));

// Upload data:
import org.apache.arrow.flight.FlightDescriptor
import org.apache.arrow.flight.AsyncPutListener
import org.apache.arrow.vector.DurationVector
import org.apache.arrow.vector.VectorSchemaRoot

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

    // upload the table into a QueryScope variable named "uploaded_table"
    FlightDescriptor descriptor = FlightDescriptor.path("scope", "uploaded_table")
    AsyncPutListener listener = new AsyncPutListener()
    FlightClient.ClientStreamListener putStream = client.startPut(descriptor, source, listener)
    putStream.putNext()
    putStream.completed()
    // ensure that the server finishes publishing the result table to the QueryScope
    listener.getResult()
}
```

On the Deephaven Server, we can run a short snippet to verify that the table was uploaded correctly:

```groovy skip-test
TableTools.show(uploaded_table.meta())
```

We can see that the destination column type is `long`:

| Name     | DataType | ColumnType | IsPartitioning |
| -------- | -------- | ---------- | -------------- |
| duration | long     | Normal     | false          |

```groovy skip-test
// print the first ten rows of the table
TableTools.show(uploaded_table)
```

We can see that the uploaded column data is represented in nanoseconds, as expected per the custom mapping implementation, even though we uploaded milliseconds:

| duration  |
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

### Download the table

Normally, you would need to set the `BaseTable.BARRAGE_SCHEMA` table attribute to configure the custom schema for the download. However, Deephaven associates the uploaded schema with a table and uses it for the download as long as we're downloading an unmodified instance of the table. Being able to round-trip from a client without setting the custom export schema is a quality-of-life feature of Deephaven.

```groovy skip-test
import org.apache.arrow.flight.FlightStream
import org.apache.arrow.flight.Ticket

Ticket ticket = new Ticket("s/uploaded_table".getBytes(StandardCharsets.UTF_8));
try (final FlightStream stream = client.getStream(ticket)) {
    while (stream.next()) {
        println(stream.getRoot().getVector(0))
    }
}
```

For the random table above, this prints:

```text
[PT0.979S, PT0.384S, PT0.186S, PT0.531S, PT0.542S, PT0.44S, PT0.576S, null, PT0.683S, PT0.699S, ... PT0.868S, null, PT0.118S, PT0.003S, PT0.356S, PT0.907S, PT0.799S, PT0.248S, PT0.372S, null]
```

## Related documentation

- [Barrage Extensions Package Summary](https://docs.deephaven.io/core/javadoc/io/deephaven/extensions/barrage/package-summary.html)
- [Arrow Flight Package Summary](https://docs.deephaven.io/core/javadoc/org/apache/arrow/flight/package-summary.html)
