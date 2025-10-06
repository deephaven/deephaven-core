---
title: InferenceResolver
sidebar_label: InferenceResolver
---

The `InferenceResolver` class provides a consolidated set of inference options for use in [`LoadTableOptions`](./load-table-options.md). This class is most useful when the caller does not know the structure of the table being loaded, and thus wants the resultant table definition to be inferred from the Iceberg table schema.

## Constructors

The `InferenceResolver` class is constructed from its [builder](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/InferenceResolver.Builder.html):

```groovy syntax
import io.deephaven.iceberg.util.InferenceResolver

resolver = InferenceResolver.builder()
    .failOnUnsupportedTypes(failOnUnsupportedTypes)
    .inferPartitioningColumns(inferPartitioningColumns)
    .namerFactory(namerFactory)
    .schema(schema)
    .build()
```

- [`failOnUnsupportedTypes`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/InferenceResolver.Builder.html#failOnUnsupportedTypes(boolean)): Whether to fail if unsupported data types are encountered during inference.
- [`inferPartitioningColumns`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/InferenceResolver.Builder.html#inferPartitioningColumns(boolean)): Whether to infer partitioning columns from the Iceberg table.
- [`namerFactory`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/InferenceResolver.Builder.html#namerFactory(io.deephaven.iceberg.util.InferenceInstructions.Namer.Factory)): The [`io.deephaven.iceberg.util.InferenceInstructions.Namer.Factory`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/InferenceInstructions.Namer.Factory.html) to use for naming columns.
- [`schema`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/InferenceResolver.Builder.html#schema(io.deephaven.iceberg.util.SchemaProvider)): The [`io.deephaven.iceberg.util.SchemaProvider`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/SchemaProvider.html) to use when extracting the schema from the Iceberg table.

## Methods

- [`walk`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/InferenceResolver.html#walk(io.deephaven.iceberg.util.ResolverProvider.Visitor)): Walk the resolver with the specified visitor.

## Examples

The following example builds an `InferenceResolver` that infers partitioning columns when loading an Iceberg table into Deephaven:

```groovy order=null
import io.deephaven.iceberg.util.InferenceResolver

resolver = InferenceResolver.builder()
    .inferPartitioningColumns(true)
    .build()
```

The following example builds an `InferenceResolver` that fails if unsupported data types are encountered when loading an Iceberg table into Deephaven:

```groovy order=null
import io.deephaven.iceberg.util.InferenceResolver

resolver = InferenceResolver.builder()
    .failOnUnsupportedTypes(true)
    .build()
```

The following example builds an `InferenceResolver` that can be used to load an Iceberg table with a given schema ID:

```groovy order=null
import io.deephaven.iceberg.util.InferenceResolver
import io.deephaven.iceberg.util.SchemaProvider

resolver = InferenceResolver.builder()
    .schema(SchemaProvider.fromSchemaId(123456789))
    .build()
```

The following example builds an `InferenceResolver` that can be used to load an Iceberg table with a given snapshot ID:

```groovy order=null
import io.deephaven.iceberg.util.InferenceResolver
import io.deephaven.iceberg.util.SchemaProvider

resolver = InferenceResolver.builder()
    .schema(SchemaProvider.fromSnapshotId(123456789))
    .build()
```

## Related documentation

- [`UnboundResolver`](./unbound-resolver.md)
- [`LoadTableOptions`](./load-table-options.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/InferenceResolver.html)
