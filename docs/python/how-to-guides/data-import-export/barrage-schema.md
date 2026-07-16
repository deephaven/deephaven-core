---
title: Barrage schema annotation
sidebar_label: Barrage schema annotation
---

Deephaven tables support Object-typed columns that can hold arbitrary Java objects. When exporting these tables over Flight using the Barrage format, Deephaven uses Apache Arrow schemas to describe the data. By default, if a column is typed as `Object`, the Arrow schema may not capture the intended structure of the data, which can lead to inefficient serialization or loss of type information. Use the `Table.BARRAGE_SCHEMA_ATTRIBUTE` to inject explicit Arrow schema information, which ensures that the Flight export uses the correct wire format.

Use this when your Deephaven column type is too generic for the intended wire type (for example, `Object` columns that should be exported as `Union` or `Map`).

## When to use schema annotation

Schema annotation is needed when:

- Exporting `Object`-typed columns that contain `Union` types (e.g., columns that may hold either `String` or `Double` values)
- Exporting `Map` columns where key/value types need explicit Arrow type definitions
- The default schema inference produces incorrect or inefficient wire formats

## How it works

1. Extract a base schema with `BarrageUtil.schemaFromTable`.
2. Replace the target field with explicit Arrow types (e.g., `ArrowType.Utf8`, `ArrowType.Union`, `ArrowType.Map`).
3. Attach the schema using [`with_attributes`](../../reference/table-operations/create/withAttributes.md).

> [!NOTE]
> `with_attributes` returns a new table. If you later transform the table (for example, with `select`, `view`, or `update`), attributes may not be preserved and you may need to re-apply the schema. Apply the schema as late as possible before export to minimize this risk.

## Supported types

The following complex Arrow types can be annotated:

- **Union** (Dense or Sparse): For columns containing multiple possible types
- **Map**: For key-value pair columns with explicit key/value type definitions
- **Nested combinations**: Maps with Union values, etc.

## Working examples

Schema annotation requires direct manipulation of Apache Arrow Java types via `jpy`. This involves careful handling of Java constructor overloads, null values, and collection types that can be complex in Python.

**For working, tested examples, see the [Groovy Barrage schema annotation guide](/core/groovy/docs/how-to-guides/data-import-export/barrage-schema).** The Groovy examples demonstrate:

- Annotating `Union<String, Double>` columns
- Annotating `Map<String, String>` columns
- Annotating `Map<String, Integer>` columns
- Annotating `Map<String, Union>` columns

The Groovy patterns can be adapted for Python use with `jpy`, but require attention to how Python maps to Java types.

## Related documentation

- [What is Barrage?](../../conceptual/what-is-barrage.md)
- [with_attributes](../../reference/table-operations/create/withAttributes.md)
- [Groovy Barrage schema annotation guide](/core/groovy/docs/how-to-guides/data-import-export/barrage-schema)
- [Arrow Flight integration](./arrow-flight.md)
