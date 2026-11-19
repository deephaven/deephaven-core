---
title: Barrage schema annotation
---

Deephaven tables support `Object`-typed columns that can hold arbitrary Java objects. When Deephaven exports a table over [Arrow Flight](./arrow-flight.md) using the [Barrage](../../conceptual/what-is-barrage.md) format, it describes the data with an Apache Arrow schema. The schema tells the client which Arrow type, or wire format, each column uses.

By default, Deephaven sends `Object`, `Map`, and other columns with no native Arrow mapping as Arrow `Utf8` strings, so the client receives strings instead of the original values. To set the wire format explicitly, put an Arrow schema in the table's [`BarrageSchema`](/core/javadoc/io/deephaven/engine/table/Table.html#BARRAGE_SCHEMA_ATTRIBUTE) attribute. In Java, the attribute name is the constant [`Table.BARRAGE_SCHEMA_ATTRIBUTE`](/core/javadoc/io/deephaven/engine/table/Table.html#BARRAGE_SCHEMA_ATTRIBUTE).

This guide shows how to:

- Export `Union` and `Map` columns with their real Arrow types.
- Opt a column into run-end or dictionary encoding.
- Check which encoding Deephaven used for each column, from a subscriber or from the server log.

## When to use schema annotation

Annotate the schema when you are:

- Exporting `Object`-typed columns that contain `Union` types (e.g., columns that may hold either `String` or `Double` values)
- Exporting `Map` columns where key/value types need explicit Arrow type definitions, including maps whose values are a `Union`
- Opting into [run-end encoding](https://arrow.apache.org/docs/format/Columnar.html#run-end-encoded-layout) to send each run of repeated values once
- Opting into [dictionary encoding](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout) to send each distinct value once
- Exporting any other column whose default Arrow type isn't the type the client needs

### Encoding auto-detection

You don't always need to annotate a column to get an encoding. Starting in version 43.0, the [server properties](../configuration/configuration-properties.md#how-to-set-properties) [`BarrageUtil.ree.autoDetectEnabled` and `BarrageUtil.dictionary.autoDetectEnabled`](../configuration/configuration-properties.md#barrage-encoding) are both on by default. In 42.x releases, both properties exist but are off by default. While either is enabled, an [Arrow Flight](./arrow-flight.md) client can receive run-end encoded or dictionary-encoded columns from a table you never annotated.

Once a table has a schema attribute, the server takes every column's encoding from that schema, whatever these properties are set to. It doesn't auto-detect an encoding for any column of that table.

## Steps to annotate a schema

These steps call Java classes that have no Python wrappers. [Get the Java classes](#get-the-java-classes) shows how to load them.

1. Get the default schema with the Java method [`BarrageUtil.schemaFromTable`](/core/javadoc/io/deephaven/extensions/barrage/util/BarrageUtil.html#schemaFromTable(io.deephaven.engine.table.Table)). It maps every column to its default Arrow type, including the `Utf8` default described above. Start from this schema and replace fields in place.
2. Replace the target field with explicit Arrow types, such as `Utf8`, `Union`, and `ArrowMap`, which [Get the Java classes](#get-the-java-classes) loads from the Java `ArrowType` class. When you re-encode a column, build its field from the Arrow type of its values instead of reusing the default field.
3. Keep one field for every column, in the table's column order. The schema sets wire types, not which columns are exported, so a schema that leaves out or reorders columns can make the export fail or pair data with the wrong field. To export fewer columns, select them on the table first.
4. Attach the schema using [`with_attributes`](../../reference/table-operations/create/withAttributes.md): `table.with_attributes({JTable.BARRAGE_SCHEMA_ATTRIBUTE: new_schema})`, where `JTable` is the Java [`Table`](/core/javadoc/io/deephaven/engine/table/Table.html) class.
5. Make attaching the schema the last step before export. Most table operations drop the attribute, as the note below lists.

If [encoding auto-detection](#encoding-auto-detection) is on, the default schema already carries the encodings the server picked for each column, based on the column's source and a sample of its current data. Attaching the schema locks in those encodings for every column you don't replace. To send a column without its inferred encoding, replace its field too.

> [!NOTE]
> [`with_attributes`](../../reference/table-operations/create/withAttributes.md) returns a new table. These operations keep the schema attribute on their result:
>
> - [Filters](../use-filters.md), such as [`where`](../../reference/table-operations/filter/where.md), [`where_in`](../../reference/table-operations/filter/where-in.md), and [`where_not_in`](../../reference/table-operations/filter/where-not-in.md)
> - [`sort`](../../reference/table-operations/sort/sort.md), [`sort_descending`](../../reference/table-operations/sort/sort-descending.md), and [`reverse`](../../reference/table-operations/sort/reverse.md)
> - [`flatten`](../../reference/table-operations/select/flatten.md)
> - [`first_by`](../../reference/table-operations/group-and-aggregate/firstBy.md), [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md), and [`agg_all_by`](../../reference/table-operations/group-and-aggregate/AggAllBy.md) with a [first](../../reference/table-operations/group-and-aggregate/AggFirst.md) or [last](../../reference/table-operations/group-and-aggregate/AggLast.md) aggregation
> - [`partition_by`](../../reference/table-operations/group-and-aggregate/partitionBy.md), which copies it to each constituent table
> - [`merge`](../../reference/table-operations/merge/merge.md), when every table it merges carries an equal schema
> - [`with_attributes`](../../reference/table-operations/create/withAttributes.md) and the other attribute methods, unless they remove it
>
> Most other operations drop it, including [`select`](../../reference/table-operations/select/select.md), [`view`](../../reference/table-operations/select/view.md), [`update`](../../reference/table-operations/select/update.md), [`head`](../../reference/table-operations/filter/head.md), and [`agg_by`](../../reference/table-operations/group-and-aggregate/aggBy.md).

## Get the Java classes

[`BarrageUtil`](/core/javadoc/io/deephaven/extensions/barrage/util/BarrageUtil.html), the Java [`Table`](/core/javadoc/io/deephaven/engine/table/Table.html) class, and the Arrow schema classes have no Python wrappers. To call them from Python, follow these rules, which [Use jpy](../use-jpy.md) covers in more detail:

- Get each class with `jpy.get_type`.
- Pass a table's `j_table` to Java methods.
- Pass `None` for a Java `null`.
- Convert a Python list to a Java list with [`j_array_list`](/core/pydoc/code/deephaven.jcompat.html#deephaven.jcompat.j_array_list).

The examples on this page use the following Java classes. Run this block before any of them:

```python test-set=barrage-schema order=null
from deephaven import empty_table
from deephaven.jcompat import j_array_list
import jpy

BarrageUtil = jpy.get_type("io.deephaven.extensions.barrage.util.BarrageUtil")
JTable = jpy.get_type("io.deephaven.engine.table.Table")
ArrayList = jpy.get_type("java.util.ArrayList")

Field = jpy.get_type("org.apache.arrow.vector.types.pojo.Field")
FieldType = jpy.get_type("org.apache.arrow.vector.types.pojo.FieldType")
Schema = jpy.get_type("org.apache.arrow.vector.types.pojo.Schema")
DictionaryEncoding = jpy.get_type(
    "org.apache.arrow.vector.types.pojo.DictionaryEncoding"
)
UnionMode = jpy.get_type("org.apache.arrow.vector.types.UnionMode")
FloatingPointPrecision = jpy.get_type(
    "org.apache.arrow.vector.types.FloatingPointPrecision"
)

# The Arrow types are nested classes of ArrowType, so their Java names use $
Utf8 = jpy.get_type("org.apache.arrow.vector.types.pojo.ArrowType$Utf8")
Int = jpy.get_type("org.apache.arrow.vector.types.pojo.ArrowType$Int")
FloatingPoint = jpy.get_type(
    "org.apache.arrow.vector.types.pojo.ArrowType$FloatingPoint"
)
Struct = jpy.get_type("org.apache.arrow.vector.types.pojo.ArrowType$Struct")
Union = jpy.get_type("org.apache.arrow.vector.types.pojo.ArrowType$Union")
ArrowMap = jpy.get_type("org.apache.arrow.vector.types.pojo.ArrowType$Map")
RunEndEncoded = jpy.get_type(
    "org.apache.arrow.vector.types.pojo.ArrowType$RunEndEncoded"
)
```

## Example: Annotate `Union<String, Double>` columns

The following example creates a table whose `Value` column holds a `String` in even rows and a `Double` in odd rows. The Arrow schema annotates the column as a [dense union](https://arrow.apache.org/docs/format/Columnar.html#dense-union) with `String` and `Double` branches. When Deephaven exports the table over [Arrow Flight](./arrow-flight.md) with [Barrage](../../conceptual/what-is-barrage.md), it sends the column as an Arrow `Union` instead of as strings.

Deephaven writes each value to the first branch whose type is the value's Java class or a supertype of it, and writes each null value to the first branch. The export fails if a non-null value matches no branch. [Sparse unions](https://arrow.apache.org/docs/format/Columnar.html#sparse-union) (`UnionMode.Sparse`) are also supported.

```python test-set=barrage-schema order=table,union_table
# Table creation: even rows hold a String, odd rows hold a Double
table = empty_table(20).update(
    [
        "Row = ii",
        "Value = ii % 2 == 0 ? (Object) (`str-` + ii) : (Object) (ii * 0.5)",
    ]
)

# Get the default schema
base_schema = BarrageUtil.schemaFromTable(table.j_table)
fields = ArrayList(base_schema.getFields())

# Define the Union types: String and Double
string_type = Field("str_val", FieldType(True, Utf8.INSTANCE, None), None)
double_type = Field(
    "double_val",
    FieldType(True, FloatingPoint(FloatingPointPrecision.DOUBLE), None),
    None,
)

# Create the Union field. Dense mode stores each value only in its own branch.
# The int array [0, 1] maps the type IDs to the child fields
union_field = Field(
    "Value",
    FieldType(True, Union(UnionMode.Dense, jpy.array("int", [0, 1])), None),
    j_array_list([string_type, double_type]),
)

# Replace the existing field with the new Union field and create a new schema
fields.set(fields.indexOf(base_schema.findField("Value")), union_field)
new_schema = Schema(fields)

# Attach the schema. with_attributes returns a new table to export and leaves the original unchanged.
union_table = table.with_attributes({JTable.BARRAGE_SCHEMA_ATTRIBUTE: new_schema})
```

`union_table` looks the same as `table` in the console, because the schema changes only how Deephaven sends the column. To see the `Union` type a client receives, run the check in [Verify the encoding from a subscriber](#verify-the-encoding-from-a-subscriber) against `union_table`.

## Example: Annotate `Map<String, String>` columns

The following example creates a table with a column of `Map<String, String>`. The Arrow schema annotates the column as an Arrow `Map` with the correct key and value types. When Deephaven exports the table over [Arrow Flight](./arrow-flight.md) with [Barrage](../../conceptual/what-is-barrage.md), it sends the column as a `Map` instead of as strings. An Arrow `Map` holds its key and value fields in a child struct, conventionally named `entries`.

```python test-set=barrage-schema order=table,map_string_table
# Table creation: each row holds a Map<String, String>
table = empty_table(20).update(
    [
        "Row = ii",
        "Map = java.util.Map.of(`Color`, ii % 2 == 0 ? `red` : `blue`, `Size`, ii % 3 == 0 ? `small` : `large`)",
    ]
)

# Get the default schema
base_schema = BarrageUtil.schemaFromTable(table.j_table)
fields = ArrayList(base_schema.getFields())

# Define key/value for Map<String, String>. Map keys can't be null.
key_field = Field("key", FieldType(False, Utf8.INSTANCE, None), None)
value_field = Field("value", FieldType(True, Utf8.INSTANCE, None), None)

# Wrap in the required entries struct
map_entries = Field(
    "entries", FieldType(False, Struct(), None), j_array_list([key_field, value_field])
)

# Create the Map field. False means the keys are not sorted.
map_field = Field(
    "Map", FieldType(True, ArrowMap(False), None), j_array_list([map_entries])
)

# Replace the existing field with the new Map field and create a new schema
fields.set(fields.indexOf(base_schema.findField("Map")), map_field)
new_schema = Schema(fields)

# Attach the schema. with_attributes returns a new table to export and leaves the original unchanged.
map_string_table = table.with_attributes({JTable.BARRAGE_SCHEMA_ATTRIBUTE: new_schema})
```

As with the `Union` example, the difference shows only on the wire.

For a `Map<String, Integer>` column, change only the value field: `value_field = Field("value", FieldType(True, Int(32, True), None), None)`.

## Example: Annotate `Map<String, Union>` columns

This example demonstrates using `Union` for values in a `Map` with `String` keys. The `Union` can contain a `Double`, `String`, `Long`, or `Integer`. Each map in the example holds one value of each type.

```python test-set=barrage-schema order=table,map_union_table
# Table creation: each map holds a String, a Double, a Long, and an Integer value
table = empty_table(20).update(
    [
        "Row = ii",
        "Map = java.util.Map.of(`Name`, (Object) (`item-` + ii), `Price`, (Object) (ii * 1.5), `Id`, (Object) ii, `Quantity`, (Object) (int) (ii % 5))",
    ]
)

# Get the default schema
base_schema = BarrageUtil.schemaFromTable(table.j_table)
fields = ArrayList(base_schema.getFields())

# Define the Union (the "value" in the Map)
string_type = Field("str_val", FieldType(True, Utf8.INSTANCE, None), None)
double_type = Field(
    "double_val",
    FieldType(True, FloatingPoint(FloatingPointPrecision.DOUBLE), None),
    None,
)
long_type = Field("long_val", FieldType(True, Int(64, True), None), None)
int_type = Field("int_val", FieldType(True, Int(32, True), None), None)

# The Union field itself (Dense mode)
union_value_field = Field(
    "value",
    FieldType(True, Union(UnionMode.Dense, jpy.array("int", [0, 1, 2, 3])), None),
    j_array_list([string_type, double_type, long_type, int_type]),
)

# Define the Map key (String/Utf8)
key_field = Field("key", FieldType(False, Utf8.INSTANCE, None), None)

# Wrap the key and Union value into Map entries
# "entries" is the conventional name for the inner Struct of an Arrow Map; the struct itself (key, then value) is required
map_entries = Field(
    "entries",
    FieldType(False, Struct(), None),
    j_array_list([key_field, union_value_field]),
)

# Create the final Map field
map_field = Field(
    "Map", FieldType(True, ArrowMap(False), None), j_array_list([map_entries])
)

# Replace the existing field with the new Map field and create a new schema
fields.set(fields.indexOf(base_schema.findField("Map")), map_field)
new_schema = Schema(fields)

# Attach the schema. with_attributes returns a new table to export and leaves the original unchanged.
map_union_table = table.with_attributes({JTable.BARRAGE_SCHEMA_ATTRIBUTE: new_schema})
```

As with the `Union` example, the difference shows only on the wire.

## Example: Run-end encode a column

[Run-end encoding (REE)](https://arrow.apache.org/docs/format/Columnar.html#run-end-encoded-layout) is a wire-level optimization for columns whose values repeat in consecutive runs. Instead of sending every value, Deephaven serializes the column as two child arrays:

- `run_ends` — a non-nullable integer array of cumulative 1-based end indices, one per run.
- `values` — the value that each run repeats, one per run.

A column of 1,000 rows where each integer repeats 100 times in a row costs 10 `run_ends` entries and 10 `values` entries instead of 1,000 integers.

REE changes only the transport. Deephaven still stores the column unencoded, with its type unchanged.

A record batch is the set of rows [Barrage](../../conceptual/what-is-barrage.md) sends in one Arrow message. Deephaven encodes each record batch separately, so the last run end equals that batch's row count.

The `run_ends` field you supply in the [`BarrageSchema`](/core/javadoc/io/deephaven/engine/table/Table.html#BARRAGE_SCHEMA_ATTRIBUTE) attribute sets the integer width of the run ends:

- `Int16` — the narrowest width, which limits each record batch to 32,767 rows (Java's `Short.MAX_VALUE`).
- `Int32` — the width auto-detection uses, which allows about 2.1 billion rows per record batch.
- `Int64` — the widest supported width.

The following example run-end encodes the `Status` column, which holds runs of `OPEN` and `CLOSED` values:

```python test-set=barrage-schema order=table,ree_table
table = empty_table(100).update(
    ["Status = (ii % 10 < 7) ? `OPEN` : `CLOSED`", "Value = (int) ii"]
)

# Get the default schema
base_schema = BarrageUtil.schemaFromTable(table.j_table)
fields = ArrayList(base_schema.getFields())

# run_ends child: non-nullable Int32 index (handles up to ~2 billion logical rows per batch)
run_ends_field = Field(
    "run_ends", FieldType(False, Int(32, True), None, None), j_array_list([])
)
# values child: the Arrow type of the values in each run (Utf8 for a String column).
# Don't reuse the default Status field here: auto-detection may have already encoded it.
values_field = Field(
    "values", FieldType(True, Utf8.INSTANCE, None, None), j_array_list([])
)
# REE parent: holds no nulls and no buffers; nulls, if any, live in the values child
ree_field = Field(
    "Status",
    FieldType(False, RunEndEncoded.INSTANCE, None, None),
    j_array_list([run_ends_field, values_field]),
)

fields.set(fields.indexOf(base_schema.findField("Status")), ree_field)
new_schema = Schema(fields)

# Attach the schema. with_attributes returns a new table to export and leaves the original unchanged.
ree_table = table.with_attributes({JTable.BARRAGE_SCHEMA_ATTRIBUTE: new_schema})
```

To confirm that Deephaven sends the column run-end encoded, see [Verify the encoding from a subscriber](#verify-the-encoding-from-a-subscriber) below.

## Example: Dictionary encode a column

[Dictionary encoding](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout) is a wire-level optimization for low-cardinality columns. Instead of sending each value in full, Deephaven sends each unique value to a subscriber once, in a `DictionaryBatch` message, and replaces each row with a compact integer index. Deephaven sends a value again only after it resets the dictionary, as the caution below describes.

A string column with 1,000 rows drawn from only 5 distinct values costs 5 full string entries in the dictionary plus 1,000 integer indices, rather than 1,000 full strings.

Dictionary encoding changes only the transport. Deephaven still stores the column unencoded, with its type unchanged.

The `DictionaryEncoding` index width controls the integer type used for indices:

- `Int8` (8-bit signed) — the most compact option, but limits a single update or snapshot to at most 128 distinct values, as the caution below explains.
- `Int16` (16-bit signed) — a compact option, but limits a single update or snapshot to at most 32,768 distinct values.
- `Int32` (32-bit signed) — the recommended option, which handles up to about 2.1 billion distinct values.
- `Int64` (64-bit signed) — an accepted option that adds no capacity over `Int32`, because Deephaven computes indices as 32-bit integers.

> [!CAUTION]
> Deephaven sends dictionary updates as deltas, so entries accumulate as new unique values appear. To prevent unbounded growth on the server and client, Deephaven resets the dictionary before writing a message in either of these cases:
>
> - The dictionary has grown larger than the table or the subscriber's [viewport](../../conceptual/what-is-barrage.md#viewports).
> - The message's new values could push the dictionary past what the index type can represent: 128 distinct values for `Int8`, or 32,768 for `Int16`.
>
> A reset flushes the current dictionary, which then accumulates only newly encountered values. The index limit therefore applies to the distinct values in one message, not to every value the column has ever held.
>
> Deephaven throws an error at serialization time if the values in a single update or snapshot exceed what the index type can represent. Use `Int8` or `Int16` only when the number of distinct values a column holds at one time stays within that limit.

The following example dictionary encodes the `Status` column, which holds only three distinct values:

```python test-set=barrage-schema order=table,dict_table
table = empty_table(100).update(
    [
        "Status = (ii % 3 == 0) ? `OPEN` : (ii % 3 == 1) ? `CLOSED` : `PENDING`",
        "Value = (int) ii",
    ]
)

# Get the default schema
base_schema = BarrageUtil.schemaFromTable(table.j_table)
fields = ArrayList(base_schema.getFields())

# Build the dictionary-encoded field:
#   type       = Utf8 (the Arrow wire type of the dictionary values; Deephaven adds the deephaven:type metadata that records the Java column type)
#   dictionary = DictionaryEncoding(id=0, ordered=False, indexType=Int32)
# The id identifies this dictionary within the stream. Use an id that no other field in the schema uses:
# with auto-detection on, the default schema may already give other columns ids 0, 1, 2, ... in column order.
# Here Status is the only String column, so no other field can hold id 0.
dict_field = Field(
    "Status",
    FieldType(True, Utf8.INSTANCE, DictionaryEncoding(0, False, Int(32, True)), None),
    j_array_list([]),
)

fields.set(fields.indexOf(base_schema.findField("Status")), dict_field)
new_schema = Schema(fields)

# Attach the schema. with_attributes returns a new table to export and leaves the original unchanged.
dict_table = table.with_attributes({JTable.BARRAGE_SCHEMA_ATTRIBUTE: new_schema})
```

To confirm that Deephaven sends `Status` dictionary encoded, run the check in [Verify the encoding from a subscriber](#verify-the-encoding-from-a-subscriber) against `dict_table`.

## Verify the encoding from a subscriber

Every subscriber receives the schema Deephaven exported the table with. A subscriber that runs the Deephaven Java engine, such as another Deephaven server that resolves the table through a [URI](../use-uris.md), stores that schema on the table it receives, in the same [`BarrageSchema`](/core/javadoc/io/deephaven/engine/table/Table.html#BARRAGE_SCHEMA_ATTRIBUTE) attribute. For a table you annotated, reading the attribute back tells you exactly which encoding Deephaven used for each column.

To check the encoding from a subscriber:

1. Run the [run-end encoded example](#example-run-end-encode-a-column) so that `ree_table` exists.
2. Make sure the server you subscribe to allows [anonymous authentication](../authentication/auth-anon.md).
3. Subscribe to `ree_table` over a [URI](../use-uris.md) with [`resolve`](../../reference/data-import-export/uri.md), from a second Deephaven instance or from the same one.
4. Read the attribute from the table that [`resolve`](../../reference/data-import-export/uri.md) returns. Don't read it from a derived table. Most operations drop the attribute, as [Steps to annotate a schema](#steps-to-annotate-a-schema) describes.

The following code subscribes and prints the encoding of each column:

```python skip-test
from deephaven.uri import resolve
import jpy

JTable = jpy.get_type("io.deephaven.engine.table.Table")
ArrowTypeID = jpy.get_type("org.apache.arrow.vector.types.pojo.ArrowType$ArrowTypeID")

# Subscribe to the exported table; client_table is a live Barrage subscription
client_table = resolve("dh+plain://localhost:10000/scope/ree_table")

# The schema the server actually exported with, as an org.apache.arrow.vector.types.pojo.Schema
wire_schema = client_table.j_table.getAttribute(JTable.BARRAGE_SCHEMA_ATTRIBUTE)

for field in wire_schema.getFields().toArray():
    ree = field.getType().getTypeID() == ArrowTypeID.RunEndEncoded
    # A dictionary lives on the field itself, or on the REE values child when doubly encoded
    values_field = field.getChildren().get(1) if ree else field
    run_ends = f" run_ends={field.getChildren().get(0).getType()}" if ree else ""
    print(
        f"{field.getName()}: run_end_encoded={str(ree).lower()}{run_ends}"
        f" dictionary_encoded={str(values_field.getDictionary() is not None).lower()}"
        f" arrow_type={values_field.getType()}"
    )
```

This prints:

```text
Status: run_end_encoded=true run_ends=Int(32, true) dictionary_encoded=false arrow_type=Utf8
Value: run_end_encoded=false dictionary_encoded=false arrow_type=Int(32, true)
```

`Status` arrived as `RunEndEncoded` with `Int32` run ends, exactly as annotated, while Deephaven sent `Value` unencoded. Running the same check against `dict_table` from the [dictionary-encoded example](#example-dictionary-encode-a-column) prints `dictionary_encoded=true` for `Status` instead.

The same check works on a static table you did not annotate: it shows which encodings [auto-detection](#encoding-auto-detection) picked. With both auto-detection properties turned off, it prints `false` for `run_end_encoded` and `dictionary_encoded` on every column.

Don't rely on this check for a ticking table you didn't annotate. The server picks encodings by sampling the table's current data each time it computes a schema, so the encoding this check reports can differ from the one the subscription uses. Subscriptions at the same update interval share one schema, computed when the first subscription started. A subscription at a different update interval gets its own. The [server log](#verify-the-encoding-from-the-server-log) records each computation. For an encoding that stays fixed, and that this check reports exactly, annotate the table.

Use `print(wire_schema.toJson())` to dump the entire export schema, including each field's `deephaven:type` metadata, which records the Deephaven column type the field came from.

> [!NOTE]
> Because both encodings are transport-only, the subscriber's `Status` column is still a `String` and its [`TableDefinition`](/core/pydoc/code/deephaven.table.html#deephaven.table.TableDefinition) is the same either way. The schema attribute is the only place that shows how Deephaven sent the bytes.

## Verify the encoding from the server log

The server also logs which encoding it chose for each column each time it computes a table's [Barrage](../../conceptual/what-is-barrage.md) schema. That happens on every export and on every direct call to [`BarrageUtil.schemaFromTable`](/core/javadoc/io/deephaven/extensions/barrage/util/BarrageUtil.html#schemaFromTable(io.deephaven.engine.table.Table)), so a table can appear in the log more than once.

To see the log, raise the level of the `io.deephaven.extensions.barrage.util.BarrageUtil` logger:

- `DEBUG` logs a one-line summary each time.
- `TRACE` also logs the complete Arrow schema.

You can raise the level in any of these ways:

- Start the server with the `logback-debug.xml` configuration described in [Enable debug logging](../logs.md#enable-debug-logging). It logs every logger at `DEBUG`, which includes the one-line summary.
- Add a `<logger>` element for this logger to your own Logback configuration file, and pass that file with `-Dlogback.configurationFile`. Use this to get `TRACE`, or to raise only this logger.
- Change the level at runtime with `ch.qos.logback.classic.Logger#setLevel`.

The following `<logger>` element, placed inside `<configuration>`, sets the level to `DEBUG`:

```xml
<logger name="io.deephaven.extensions.barrage.util.BarrageUtil" level="DEBUG"/>
```

The log names each table by its [`BarragePerformanceTableKey`](/core/javadoc/io/deephaven/engine/table/Table.html#BARRAGE_PERFORMANCE_KEY_ATTRIBUTE) attribute, which [Barrage metrics for performance monitoring](../performance/barrage-performance.md) describes. If the table has no such attribute, the log uses the table description. For a table whose [`BarragePerformanceTableKey`](/core/javadoc/io/deephaven/engine/table/Table.html#BARRAGE_PERFORMANCE_KEY_ATTRIBUTE) attribute is `orders`, the log lines look like this, with the timestamp and thread columns omitted:

```text
DEBUG | i.d.e.b.util.BarrageUtil | Barrage schema for orders: 2 columns, encodings from explicit BarrageSchema: Status=REE(INT32)
TRACE | i.d.e.b.util.BarrageUtil | Barrage schema for orders: Schema<Status: RunEndEncoded<run_ends: Int(32, true) not null, values: Utf8> not null, Value: Int(32, true)>(metadata: {...})
```

These lines use the default `logback.xml` layout. Under `logback-debug.xml`, the logger column shows the full class name.

The summary reports where the encodings came from: an explicit [`BarrageSchema`](/core/javadoc/io/deephaven/engine/table/Table.html#BARRAGE_SCHEMA_ATTRIBUTE) attribute or auto-detection. Use it to confirm what the server chose for a table you did not annotate yourself.

## Related documentation

- [What is Barrage?](../../conceptual/what-is-barrage.md)
- [Arrow Flight and Deephaven](./arrow-flight.md)
- [Use URIs to share tables](../use-uris.md)
- [Use jpy](../use-jpy.md)
- [Filter table data](../use-filters.md)
- [Enable anonymous authentication](../authentication/auth-anon.md)
- [Barrage metrics for performance monitoring](../performance/barrage-performance.md)
- [Retrieve logs](../logs.md)
- [Configuration properties reference](../configuration/configuration-properties.md)
- [`agg.first`](../../reference/table-operations/group-and-aggregate/AggFirst.md)
- [`agg.last`](../../reference/table-operations/group-and-aggregate/AggLast.md)
- [`agg_all_by`](../../reference/table-operations/group-and-aggregate/AggAllBy.md)
- [`agg_by`](../../reference/table-operations/group-and-aggregate/aggBy.md)
- [`empty_table`](../../reference/table-operations/create/emptyTable.md)
- [`first_by`](../../reference/table-operations/group-and-aggregate/firstBy.md)
- [`flatten`](../../reference/table-operations/select/flatten.md)
- [`head`](../../reference/table-operations/filter/head.md)
- [`last_by`](../../reference/table-operations/group-and-aggregate/lastBy.md)
- [`merge`](../../reference/table-operations/merge/merge.md)
- [`partition_by`](../../reference/table-operations/group-and-aggregate/partitionBy.md)
- [`resolve`](../../reference/data-import-export/uri.md)
- [`reverse`](../../reference/table-operations/sort/reverse.md)
- [`select`](../../reference/table-operations/select/select.md)
- [`sort`](../../reference/table-operations/sort/sort.md)
- [`sort_descending`](../../reference/table-operations/sort/sort-descending.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`view`](../../reference/table-operations/select/view.md)
- [`where`](../../reference/table-operations/filter/where.md)
- [`where_in`](../../reference/table-operations/filter/where-in.md)
- [`where_not_in`](../../reference/table-operations/filter/where-not-in.md)
- [`with_attributes`](../../reference/table-operations/create/withAttributes.md)
- [`j_array_list` Pydoc](/core/pydoc/code/deephaven.jcompat.html#deephaven.jcompat.j_array_list)
- [`TableDefinition` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.TableDefinition)
- [`BarrageUtil` Javadoc](/core/javadoc/io/deephaven/extensions/barrage/util/BarrageUtil.html)
- [`Table` Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
