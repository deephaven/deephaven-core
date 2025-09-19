---
title: IcebergUpdateMode
sidebar_label: IcebergUpdateMode
---

The `IcebergUpdateMode` class defines the modes available for updating Deephaven tables that are backed by Iceberg tables. The update modes determine how the Deephaven table reflects changes made to the underlying Iceberg table.

## Constructors

An instance of `IcebergUpdateMode` is created using one of the class's static methods:

```groovy syntax
import io.deephaven.iceberg.util.IcebergUpdateMode

autoRefreshingMode = IcebergUpdateMode.autoRefreshingMode()
autoRefreshingModeRefreshInterval = IcebergUpdateMode.autoRefreshingMode(refreshMs)
manualRefreshingMode = IcebergUpdateMode.manualRefreshingMode()
staticMode = IcebergUpdateMode.staticMode()
```

## Parameters

- `refreshMs`: The milliseconds between refreshes when using the auto-refreshing mode. If not specified, the default is 60,000 milliseconds (1 minute).

## Methods

The `IcebergUpdateMode` class provides the following methods:

- [`autoRefreshMs`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergUpdateMode.html#autoRefreshMs()): Set the refresh interval in milliseconds for the auto-refreshing mode. Use of this method is only necessary if auto-refreshing mode was created with the zero-argument constructor.
- [`updateType`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergUpdateMode.html#updateType()): Set the update mode for the resultant table. The default is static mode.

## Examples

The following code block constructs update modes for static, manual refreshing, and auto-refreshing Iceberg tables:

```groovy
import io.deephaven.iceberg.util.IcebergUpdateMode

staticMode = IcebergUpdateMode.staticMode()
manualRefreshingMode = IcebergUpdateMode.manualRefreshingMode()
autoRefreshingMode = IcebergUpdateMode.autoRefreshingMode()
autoRefreshingMode30s = IcebergUpdateMode.autoRefreshingMode(30000)
```

## Related documentation

- [Deephaven and Iceberg](../../../how-to-guides/data-import-export/iceberg.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergReadInstructions`](./iceberg-read-instructions.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergUpdateMode.html)
