---
title: Barrage schema annotation
sidebar_label: Barrage schema annotation
---

Deephaven tables support Object-typed columns that can hold arbitrary Java objects. When exporting these tables over Flight using the Barrage format, Deephaven uses Apache Arrow schemas to describe the data. By default, if a column is typed as `Object`, the Arrow schema may not capture the intended structure of the data, which can lead to inefficient serialization or loss of type information. Use the `Table.BARRAGE_SCHEMA_ATTRIBUTE` to inject explicit Arrow schema information, which ensures that the Flight export uses the correct wire format.

Use this when your Deephaven column type is too generic for the intended wire type (for example, `Object` columns that should be exported as `Union` or `Map`), or when you want to opt into a wire-level compression such as Run-End Encoding. This guide includes examples of the `Union`, `Map`, and `RunEndEncoded` types, which are supported by Deephaven.

## How It Works

1. Extract a base schema with `BarrageUtil.schemaFromTable(...)`. Manages basic type mapping for primitive types and collections of primitives.
2. Replace the target field with explicit Arrow types.
3. Attach the schema using `withAttributes(Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, newSchema))`.

> [!NOTE]
> `withAttributes(...)` returns a new table. If you later transform the table (for example, with `select`, `view`, or `update`), attributes may not be preserved and you may need to re-apply the schema. Ideally, you would apply the schema as late as possible before export to minimize this risk.

## Example: Annotate `Union<String, Double>` Columns

The following example creates a table with a column of Objects (limited for this example to `String` and `Double`). The Arrow schema annotates the column as a dense union with `String` and `Double` branches. The final table can be exported over Flight / Barrage without error.

```groovy order=table,table_w_attributes
// Table creation

import java.util.Random

QueryScope.addParam("rnd", new Random())
QueryScope.addParam("ALPHA_NUMERIC_CHARS", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray())
QueryScope.addParam("rndString", (len) -> {
    char[] buffer = new char[len];
    for (int i = 0; i < len; i++) {
        buffer[i] = ALPHA_NUMERIC_CHARS[rnd.nextInt(ALPHA_NUMERIC_CHARS.length)];
    }
    return new String(buffer);
})
// Randomly return either a String or a Double (as an Object)
QueryScope.addParam("rndObject", () -> {
    if (rnd.nextBoolean()) {
        return (Object)rndString(5);
    } else {
        return (Object)rnd.nextDouble();
    }
})
table = emptyTable(20).update("row = ii", "rnd = rndObject()")

// Schema annotation

import io.deephaven.extensions.barrage.util.BarrageUtil
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.UnionMode
import org.apache.arrow.vector.types.FloatingPointPrecision
import io.deephaven.engine.table.Table

// 1. Get existing schema
def curr_schema = BarrageUtil.schemaFromTable(table)
def fields = new ArrayList<>(curr_schema.getFields())

// 2. Define the Union types: String and Double
def stringType = new Field("str_val", new FieldType(true, ArrowType.Utf8.INSTANCE, null), null)
def doubleType = new Field("double_val", new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null), null)

// 3. Create the Union field.  We use Dense union mode here (more common for Barrage memory efficiency)
// The int array [0, 1] maps the type IDs to the child fields
def unionFieldName = fields[1].name
def unionField = new Field(
    unionFieldName,
    new FieldType(true, new ArrowType.Union(UnionMode.Dense, [0, 1] as int[]), null),
    [stringType, doubleType]
)

// 4. Replace the existing field with the new Union field and create a new schema
fields.set(1, unionField)
def new_schema = new Schema(fields)

// 5. Apply attributes, creating a new table reference which can be used for export; the original table is unchanged
table_w_attributes = table.withAttributes(java.util.Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, new_schema))
```

## Example: Annotate `Map<String, String>` Columns

The following example creates a table with a column of `Map<String, String>`. The Arrow schema annotates the column as an Arrow `Map` with the correct types for key and values. The final table can be exported over Flight / Barrage without error.

```groovy order=table,table_w_attributes
// Table creation

import java.util.Random

QueryScope.addParam("rnd", new Random())
QueryScope.addParam("ALPHA_NUMERIC_CHARS", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray())
QueryScope.addParam("rndString", (len) -> {
    char[] buffer = new char[len];
    for (int i = 0; i < len; i++) {
        buffer[i] = ALPHA_NUMERIC_CHARS[rnd.nextInt(ALPHA_NUMERIC_CHARS.length)];
    }
    return new String(buffer);
})
QueryScope.addParam("rndMapStringString", () -> {
    return Map.of(
        rndString(5), rndString(5),
        rndString(5), rndString(5),
        rndString(5), rndString(5),
        rndString(5), rndString(5)
    )
})

table = emptyTable(20).update("row = ii", "map = rndMapStringString()")

// Schema annotation

import io.deephaven.extensions.barrage.util.BarrageUtil
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.ArrowType
import io.deephaven.engine.table.Table

// 1. Get existing schema
def curr_schema = BarrageUtil.schemaFromTable(table)
def fields = new ArrayList<>(curr_schema.getFields())

// 2. Define key/value for Map<String, String>
def keyField = new Field("key", new FieldType(false, ArrowType.Utf8.INSTANCE, null), null)
def valueField = new Field("value", new FieldType(true, ArrowType.Utf8.INSTANCE, null), null)

// 3. Wrap in the required entries struct
def mapEntries = new Field(
    "entries",
    new FieldType(false, new ArrowType.Struct(), null),
    [keyField, valueField]
)

// 4. Create the Map field
def mapFieldName = fields[1].name
def mapField = new Field(
    mapFieldName,
    new FieldType(true, new ArrowType.Map(false), null),
    [mapEntries]
)

// 5. Replace the existing field with the new Map field and create a new schema
fields.set(1, mapField)
def new_schema = new Schema(fields)

// 6. Apply attributes, creating a new table reference which can be used for export; the original table is unchanged
table_w_attributes = table.withAttributes(java.util.Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, new_schema))
```

## Example: Annotate `Map<String, Integer>` Columns

The following example creates a table with a column of `Map<String, Integer>`. The Arrow schema annotates the column as an Arrow `Map` with `String` keys and `Integer` values. The final table can be exported over Flight / Barrage without error.

```groovy order=table,table_w_attributes
// Table creation

import java.util.Random

QueryScope.addParam("rnd", new Random())
QueryScope.addParam("ALPHA_NUMERIC_CHARS", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray())
QueryScope.addParam("rndString", (len) -> {
    char[] buffer = new char[len];
    for (int i = 0; i < len; i++) {
        buffer[i] = ALPHA_NUMERIC_CHARS[rnd.nextInt(ALPHA_NUMERIC_CHARS.length)];
    }
    return new String(buffer);
})
QueryScope.addParam("rndMapStringInteger", () -> {
    return Map.of(
        rndString(5), rnd.nextInt(1000),
        rndString(5), rnd.nextInt(1000),
        rndString(5), rnd.nextInt(1000),
        rndString(5), rnd.nextInt(1000)
    )
})

table = emptyTable(20).update("row = ii", "map = rndMapStringInteger()")

// Schema annotation

import io.deephaven.extensions.barrage.util.BarrageUtil
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.ArrowType
import io.deephaven.engine.table.Table

// 1. Get existing schema
def curr_schema = BarrageUtil.schemaFromTable(table)
def fields = new ArrayList<>(curr_schema.getFields())

// 2. Define key/value for Map<String, Integer>
def keyField = new Field("key", new FieldType(false, ArrowType.Utf8.INSTANCE, null), null)
def valueField = new Field("value", new FieldType(true, new ArrowType.Int(32, true), null), null)

// 3. Wrap in the required entries struct
def mapEntries = new Field(
    "entries",
    new FieldType(false, new ArrowType.Struct(), null),
    [keyField, valueField]
)

// 4. Create the Map field
def mapFieldName = fields[1].name
def mapField = new Field(
    mapFieldName,
    new FieldType(true, new ArrowType.Map(false), null),
    [mapEntries]
)

// 5. Replace the existing field with the new Map field and create a new schema
fields.set(1, mapField)
def new_schema = new Schema(fields)

// 6. Apply attributes, creating a new table reference which can be used for export; the original table is unchanged
table_w_attributes = table.withAttributes(java.util.Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, new_schema))
```

## Example: Annotate `Map<String, Union>` Columns

This example demonstrates the use of `Union` for values in a `Map` with `String` keys. The `Union` can contain a `Double`, `String`, `Long`, or `Integer`.

```groovy order=table,table_w_attributes
// Table creation

import java.util.Random

QueryScope.addParam("rnd", new Random())
QueryScope.addParam("ALPHA_NUMERIC_CHARS", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray())
QueryScope.addParam("rndString", (len) -> {
    char[] buffer = new char[len];
    for (int i = 0; i < len; i++) {
        buffer[i] = ALPHA_NUMERIC_CHARS[rnd.nextInt(ALPHA_NUMERIC_CHARS.length)];
    }
    return new String(buffer);
})
QueryScope.addParam("rndObject", () -> {
    int choice = rnd.nextInt(4);
    switch(choice) {
        case 0:
            return (Object)rndString(5);
        case 1:
            return (Object)rnd.nextDouble();
        case 2:
            return (Object)rnd.nextLong();
        case 3:
            return (Object)rnd.nextInt();
        default:
            return (Object)rndString(5);
    }
})
QueryScope.addParam("rndMapStringUnion", (len) -> {
    return Map.of(
        rndString(5), rndObject(),
        rndString(5), rndObject(),
        rndString(5), rndObject(),
        rndString(5), rndObject(),
        rndString(5), rndObject(),
        rndString(5), rndObject()
    )
})

table = emptyTable(20).update("row = ii", "map = rndMapStringUnion()")

// Schema annotation

import io.deephaven.extensions.barrage.util.BarrageUtil
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.UnionMode
import org.apache.arrow.vector.types.FloatingPointPrecision
import io.deephaven.engine.table.Table

// 1. Get existing schema
def curr_schema = BarrageUtil.schemaFromTable(table)
def fields = new ArrayList<>(curr_schema.getFields())

// 2. Define the Union (The "Value" in the Map)
def stringType = new Field("str_val", new FieldType(true, ArrowType.Utf8.INSTANCE, null), null)
def doubleType = new Field("double_val", new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null), null)
def longType = new Field("long_val", new FieldType(true, new ArrowType.Int(64, true), null), null)
def intType = new Field("int_val", new FieldType(true, new ArrowType.Int(32, true), null), null)

// The Union field itself (Dense mode)
def unionValueField = new Field(
    "value",
    new FieldType(true, new ArrowType.Union(UnionMode.Dense, [0, 1, 2, 3] as int[]), null),
    [stringType, doubleType, longType, intType]
)

// 3. Define the Map Key (String/Utf8)
def keyField = new Field("key", new FieldType(false, ArrowType.Utf8.INSTANCE, null), null)

// 4. Wrap Key and Union-Value into Map Entries
// "entries" is the mandatory name for the inner Struct of an Arrow Map
def mapEntries = new Field(
    "entries",
    new FieldType(false, new ArrowType.Struct(), null),
    [keyField, unionValueField]
)

// 5. Create the Final Map Field
def mapFieldName = fields[1].name
def mapField = new Field(
    mapFieldName,
    new FieldType(true, new ArrowType.Map(false), null),
    [mapEntries]
)

// 6. Replace the existing field with the new Map field and create a new schema
fields.set(1, mapField)
def new_schema = new Schema(fields)

// 7. Apply attributes, creating a new table reference which can be used for export; the original table is unchanged
table_w_attributes = table.withAttributes(java.util.Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, new_schema))
```

## Example: Run-End Encoded (REE) Columns

[Run-End Encoding](https://arrow.apache.org/docs/format/Columnar.html#run-end-encoded-layout) is a wire-level optimization for columns with many repeated values. Instead of sending every value, the column is serialized as two child arrays:

- `run_ends` — a non-nullable integer array of cumulative 1-based end indices, one per run. The last value always equals the logical row count.
- `values` — the values that will be repeated in the run.

A column of 1,000 rows where the same integer repeats 100 times in a row costs 10 `run_end` entries + 10 `value` entries instead of 1,000 integers. Deephaven stores the column flat (unchanged type); REE is a transport-only optimization. The `run_ends` integer width is determined by the Arrow field structure you supply via `BARRAGE_SCHEMA_ATTRIBUTE`. Use `Int32` unless you have a specific reason to use `Int16`. Note that `Int16` `run_ends` constrain the effective batch size to at most `Short.MAX_VALUE` / 32,767 rows per record batch.

```groovy order=table,table_w_attributes
import io.deephaven.engine.table.Table
import io.deephaven.extensions.barrage.util.BarrageUtil
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema

table = emptyTable(100).update(
    "status = (ii % 10 < 7) ? `OPEN` : `CLOSED`",
    "value  = (int) ii"
)

// Extract the default schema to borrow existing field metadata
def baseSchema = BarrageUtil.schemaFromTable(table)
def fields = new java.util.ArrayList<>(baseSchema.getFields())

// run_ends child: non-nullable Int32 index (handles up to ~2 billion logical rows per batch)
def runEndsField = new Field("run_ends",
    new FieldType(false, new ArrowType.Int(32, true), null, null),
    java.util.Collections.emptyList()
)
// values child: reuse the original "status" field type so deephaven:type metadata is preserved
def originalStatusField = baseSchema.findField("status")
def valuesField = new Field("values",
    originalStatusField.getFieldType(),
    originalStatusField.getChildren()
)
// REE parent: nullable, no buffers (the children carry all the data)
def reeField = new Field("status",
    new FieldType(true, ArrowType.RunEndEncoded.INSTANCE, null, null),
    java.util.List.of(runEndsField, valuesField)
)

def statusIdx = fields.findIndexOf { it.getName() == "status" }
fields.set(statusIdx, reeField)
def new_schema = new Schema(fields)

// Apply attributes, creating a new table reference which can be used for export; the original table is unchanged
table_w_attributes = table.withAttributes(java.util.Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, new_schema))
```

To confirm that the column really is sent run-end encoded, see [Verify the encoding from a subscriber](#verify-the-encoding-from-a-subscriber) below.

## Example: Dictionary-Encoded Columns

[Dictionary Encoding](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout) is a wire-level optimization for low-cardinality columns. Instead of sending each value in full, Deephaven sends each unique value once (in a `DictionaryBatch` message) and replaces each row with a compact integer index.

A string column with 1,000 rows drawn from only 5 distinct values costs 5 full string entries (in the dictionary) + 1,000 integer indices, rather than 1,000 full strings. Deephaven stores the column flat (unchanged type); dictionary encoding is a transport-only optimization.

The `DictionaryEncoding` index width controls the integer type used for indices:

- `Int32` (32-bit signed) — handles up to about 1 billion distinct values; suitable for almost all use cases.
- `Int8` (8-bit signed) — the most compact option, but limits the dictionary to at most 128 distinct values.
- `Int16` (16-bit signed) — more compact than `Int32`, but limits the dictionary to at most 32,768 distinct values.
- `Int64` (64-bit signed) — rarely needed; use only when distinct values exceed 1 billion.

:::caution
Dictionary updates are sent as deltas, so entries accumulate as new unique values appear. To prevent unbounded growth on the server and client, Deephaven resets the dictionary when its size exceeds the table or viewport size by flushing the current dictionary and accumulating only newly encountered values. Despite this safety net, if a single table (or viewport) contains more distinct values than the index type can represent (128 for `Int8`, 32,768 for `Int16`), Deephaven throws an error at serialization time. Prefer `Int32` unless you are certain the column's active cardinality stays within the smaller limit.
:::

```groovy order=table,table_w_attributes
import io.deephaven.extensions.barrage.util.BarrageUtil
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.DictionaryEncoding
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema

table = emptyTable(100).update(
    "status = (ii % 3 == 0) ? `OPEN` : (ii % 3 == 1) ? `CLOSED` : `PENDING`",
    "value  = (int) ii"
)

// Extract the default schema to borrow existing field metadata (e.g. deephaven:type tags)
def baseSchema = BarrageUtil.schemaFromTable(table)
def fields = new ArrayList<>(baseSchema.getFields())

// Build the dictionary-encoded field:
//   type       = Utf8 (the value type of the column, so DH knows the Java column type is String)
//   dictionary = DictionaryEncoding(id=0, ordered=false, indexType=Int32)
// The id uniquely identifies this dictionary within the stream. If you encode multiple columns,
// give each a distinct id (0, 1, 2, ...).
def originalStatusField = baseSchema.findField("status")
def dictField = new Field("status",
    new FieldType(true, originalStatusField.getType(),
        new DictionaryEncoding(0L, false, new ArrowType.Int(32, true)),
        originalStatusField.getMetadata()),
    originalStatusField.getChildren()
)

def statusIdx = fields.findIndexOf { it.getName() == "status" }
fields.set(statusIdx, dictField)
def new_schema = new Schema(fields)

// Apply attributes, creating a new table reference which can be used for export; the original table is unchanged
table_w_attributes = table.withAttributes(Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, new_schema))
```

## Verify the encoding from a subscriber

The schema a table is exported with is sent to every subscriber, and Deephaven stores it on the resulting client-side table under the same `Table.BARRAGE_SCHEMA_ATTRIBUTE`. Reading that attribute back tells you exactly which encoding each column was sent with.

Run the [Run-End Encoded example](#example-run-end-encoded-ree-columns) above so that `table_w_attributes` exists, then subscribe to it — from a second Deephaven instance, or from the same instance over a [URI](../use-uris.md):

```groovy skip-test
import io.deephaven.engine.table.Table
import org.apache.arrow.vector.types.pojo.ArrowType
import static io.deephaven.uri.ResolveTools.resolve

// Subscribe to the exported table; `client_table` is a live Barrage subscription
client_table = resolve("dh+plain://localhost:10000/scope/table_w_attributes")

// The schema the server actually exported with, as an org.apache.arrow.vector.types.pojo.Schema
wire_schema = client_table.getAttribute(Table.BARRAGE_SCHEMA_ATTRIBUTE)

for (field in wire_schema.getFields()) {
    boolean ree = field.getType().getTypeID() == ArrowType.ArrowTypeID.RunEndEncoded
    // A dictionary lives on the field itself, or on the REE `values` child when doubly encoded
    def valuesField = ree ? field.getChildren().get(1) : field
    println "${field.getName()}: run_end_encoded=${ree}" +
        (ree ? " run_ends=${field.getChildren().get(0).getType()}" : "") +
        " dictionary_encoded=${valuesField.getDictionary() != null}" +
        " arrow_type=${valuesField.getType()}"
}
```

This prints:

```text
status: run_end_encoded=true run_ends=Int(32, true) dictionary_encoded=false arrow_type=Utf8
value: run_end_encoded=false dictionary_encoded=false arrow_type=Int(32, true)
```

`status` arrived as `RunEndEncoded` with `Int32` run ends, exactly as annotated, while `value` was sent unencoded. Running the same check against the [dictionary-encoded example](#example-dictionary-encoded-columns) prints `dictionary_encoded=true` for `status` instead.

Subscribing to a table with no `BARRAGE_SCHEMA_ATTRIBUTE` prints `false` for both facets of every column, unless the server has encoding auto-detection enabled. The `BarrageUtil.ree.autoDetectEnabled` and `BarrageUtil.dictionary.autoDetectEnabled` properties are both off by default; when either is set, the server may choose an encoding on its own for a table you never annotated, and this check is how you see what it picked.

Use `println wire_schema.toJson()` to dump the entire negotiated schema, including each field's `deephaven:type` metadata.

> [!NOTE]
> These encodings do not change the Deephaven column type — the subscriber's `status` column is still a `String`, and the subscriber's `TableDefinition` is identical either way. Both encodings are transport-only optimizations, so the schema attribute is the only thing that tells you how the bytes were sent.

> [!CAUTION]
> The attribute is only propagated through a few operations (`where`, `firstBy`, `lastBy`, `partitionBy`, `reverse`, `sort`, and flatten). Read it from the table returned by `resolve` rather than from a derived table.

### From the producer

The server logs the same decision for every table it exports. Raise the level of the `io.deephaven.extensions.barrage.util.BarrageUtil` logger — in your logging configuration, or at runtime with `ch.qos.logback.classic.Logger#setLevel` — to `DEBUG` for a one-line summary per export, or to `TRACE` to also dump the complete Arrow schema:

```xml
<logger name="io.deephaven.extensions.barrage.util.BarrageUtil" level="DEBUG"/>
```

```text
DEBUG | i.d.e.b.util.BarrageUtil | Barrage schema for orders: 2 columns, encodings from explicit BarrageSchema: status=REE(INT32)
TRACE | i.d.e.b.util.BarrageUtil | Barrage schema for orders: Schema<status: RunEndEncoded<run_ends: Int(32, true) not null, values: Utf8>, value: Int(32, true)>
```

The summary reports where the encodings came from — an explicit `BARRAGE_SCHEMA_ATTRIBUTE`, or auto-detection — which is how you confirm what the server chose for a table you did not annotate yourself. Tables are named by their `Table.BARRAGE_PERFORMANCE_KEY_ATTRIBUTE` when it is set, and by the table description otherwise.

## Related documentation

- [What is Barrage?](../../conceptual/what-is-barrage.md)
- [Deephaven URIs](../use-uris.md)
- [withAttributes](../../reference/table-operations/select/withAttributes.md)
- [Arrow Flight integration](./arrow-flight.md)
