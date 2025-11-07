---
title: LoadTableOptions
sidebar_label: LoadTableOptions
---

The `LoadTableOptions` class specifies options for loading Iceberg tables into Deephaven.

## Constructors

The `LoadTableOptions` class is constructed via its [builder](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/LoadTableOptions.Builder.html):

```groovy syntax
import io.deephaven.iceberg.util.*

options = LoadTableOptions.builder()
    .id(id)
    .nameMapping(nameMapping)
    .resolver(resolver)
    .build()
```

## Parameters

- `id`: A string or `org.apache.iceberg.catalog.TableIdentifier` that identifies the Iceberg table to load.
- `nameMapping`: A [`NameMappingProvider`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/NameMappingProvider.html) or `org.apache.iceberg.mapping.NameMapping` that provides a mapping of Iceberg column names to Deephaven column names.
- `resolver`: A [`ResolverProvider`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/ResolverProvider.html) that specifies how to resolve schema and partitioning information when loading the table.

## Methods

- [`id`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/LoadTableOptions.html#id()): Return the table identifier.
- [`nameMapping`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/LoadTableOptions.html#nameMapping()): Return the name mapping provider. Set to [`NameMappingProvider.fromTable`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/NameMappingProvider.html#fromTable()) by default.
- [`resolver`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/LoadTableOptions.html#resolver()): Return the resolver provider.

## Examples

The following example constructs a `LoadTableOptions` instance with a specific table identifier and default name mapping and resolver:

```groovy
import io.deephaven.iceberg.util.*

options = LoadTableOptions.builder()
    .id("my_table")
    .build()
```

## Related documentation

- [Deephaven and Iceberg](../../../how-to-guides/data-import-export/iceberg.md)
- [`BuildCatalogOptions`](./build-catalog-options.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/LoadTableOptions.html)
