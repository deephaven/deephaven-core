---
title: Barrage Schema Annotation
sidebar_label: Barrage Schema Annotation
---

Deephaven tables support Object-typed columns that can hold arbitrary Java objects. When exporting these tables over Flight using the Barrage format, Deephaven uses Apache Arrow schemas to describe the data. By default, if a column is typed as `Object`, the Arrow schema may not capture the intended structure of the data, which can lead to inefficient serialization or loss of type information. Use the `Table.BARRAGE_SCHEMA_ATTRIBUTE` to inject explicit Arrow schema information, which ensures that the Flight export uses the correct wire format.

Use this when your Deephaven column type is too generic for the intended wire type (for example, `Object` columns that should be exported as `Union` or `Map`). This guide includes examples of the `Union` and `Map` types, which are currently supported by Deephaven.

## How It Works

1. Extract a base schema with `BarrageUtil.schemaFromTable(...)`. Manages basic type mapping for primitive types and collections of primitives.
2. Replace the target field with explicit Arrow types.
3. Attach the schema using `withAttributes(Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, newSchema))`.

> [!NOTE]
> `withAttributes(...)` returns a new table. If you later transform the table (for example, with `select`, `view`, or `update`), attributes may not be preserved and you may need to re-apply the schema. Ideally, you would apply the schema as late as possible before export to minimize this risk.

## Example: Annotate `Union<String, Double>` Columns

The following example creates a table with a column of Objects (limited for this example to `String` and `Double`). The Arrow schema annotates the column as a dense union with `String` and `Double` branches. The final table can be exported over Flight / Barrage without error.

```groovy order=union_table,union_table_w_attributes
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
union_table = emptyTable(20).update("row = ii", "rnd = rndObject()")

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
def curr_schema = BarrageUtil.schemaFromTable(union_table)
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

// 5. Apply attributes, creating a new table reference which can be used for export
union_table_w_attributes = union_table.withAttributes(java.util.Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, new_schema))
```

## Example: Annotate `Map<String, String>` Columns

The following example creates a table with a column of `Map<String, Double>`. The Arrow schema annotates the column as an Arrow `Map` with the correct types for key and values. The final table can be exported over Flight / Barrage without error.

```groovy order=map_string_table,map_string_table_w_attributes
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

map_string_table = emptyTable(20).update("row = ii", "map = rndMapStringString()")

// Schema annotation

import io.deephaven.extensions.barrage.util.BarrageUtil
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.ArrowType
import io.deephaven.engine.table.Table

// 1. Get existing schema
def curr_schema = BarrageUtil.schemaFromTable(map_string_table)
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

// 6. Apply attributes, creating a new table reference which can be used for export
map_string_table_w_attributes = map_string_table.withAttributes(java.util.Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, new_schema))
```

## Example: Annotate `Map<String, Integer>` Columns

The following example creates a table with a column of `Map<String, Integer>`. The Arrow schema annotates the column as an Arrow `Map` with `String` keys and `Integer` values. The final table can be exported over Flight / Barrage without error.

```groovy order=map_string_integer_table,map_string_integer_table_w_attributes
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

map_string_integer_table = emptyTable(20).update("row = ii", "map = rndMapStringInteger()")

// Schema annotation

import io.deephaven.extensions.barrage.util.BarrageUtil
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.ArrowType
import io.deephaven.engine.table.Table

// 1. Get existing schema
def curr_schema = BarrageUtil.schemaFromTable(map_string_integer_table)
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

// 6. Apply attributes, creating a new table reference which can be used for export
map_string_integer_table_w_attributes = map_string_integer_table.withAttributes(java.util.Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, new_schema))
```

## Example: Annotate `Map<String, Union>` Columns

This example demonstrates the use of `Union` for values in a `Map` with `String` keys. The `Union` can contain a `Double`, `String`, `Long`, or `Integer`.

```groovy order=map_union_table,map_union_table_w_attributes
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

map_union_table = emptyTable(20).update("row = ii", "map = rndMapStringUnion()")

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
def curr_schema = BarrageUtil.schemaFromTable(map_union_table)
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

// 7. Apply attributes, creating a new table reference which can be used for export
map_union_table_w_attributes = map_union_table.withAttributes(java.util.Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, new_schema))
```
