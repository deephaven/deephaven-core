---
title: BuildCatalogOptions
sidebar_label: BuildCatalogOptions
---

The `BuildCatalogOptions` class specifies options to use when constructing an [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md).

## Constructors

The `BuildCatalogOptions` class is constructed via its [builder](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/BuildCatalogOptions.Builder.html):

```groovy syntax
import io.deephaven.iceberg.util.*

options = BuildCatalogOptions.builder()
    .enablePropertyInjection(boolean enable)
    .name(String name)
    .putAllHadoopConfig(Map<String, ? extends String> entries)
    .putAllProperties(Map<String, ? extends String> entries)
    .putHadoopConfig(String key, String value)
    .putProperties(String key, String value)
    .build()
```

## Parameters

When building an instance of `BuildCatalogOptions`, the following parameters can be specified:

- `enablePropertyInjection`: A boolean flag to enable or disable property injection.
- `name`: The name of the catalog adapter.
- `entries`: A map of entries to add to the Hadoop configuration or catalog properties.
- `key`: A key corresponding to a particular Hadoop configuration or catalog property.
- `value`: A value corresponding to a particular Hadoop configuration or catalog property.

## Methods

- [`enablePropertyInjection`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/BuildCatalogOptions.html#enablePropertyInjection()): Enable property injection for a catalog.
- [`hadoopConfig`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/BuildCatalogOptions.html#hadoopConfig()): List the Hadoop configuration properties.
- [`name`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/BuildCatalogOptions.html#name()): Return the name of the catalog.
- [`properties`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/BuildCatalogOptions.html#properties()): List the user-defined catalog properties.

## Examples

The following example constructs a `BuildCatalogOptions` via its builder:

```groovy
import io.deephaven.iceberg.util.BuildCatalogOptions

options = BuildCatalogOptions.builder()
    .enablePropertyInjection(true)
    .name("myCatalog")
    .putProperties("type", "rest")
    .putProperties("uri", "http://rest:8181")
    .build()
```

## Related documentation

- [Deephaven and Iceberg](../../../how-to-guides/data-import-export/iceberg.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`LoadTableOptions`](./load-table-options.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/BuildCatalogOptions.html)
