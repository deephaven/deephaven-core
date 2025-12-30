---
title: IcebergReadInstructions
sidebar_label: IcebergReadInstructions
---

The `IcebergReadInstructions` class provides instructions for reading Iceberg catalogs and tables.

## Constructors

An `IcebergReadInstructions` object is constructed using its [builder](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.Builder.html):

```groovy syntax
import io.deephaven.iceberg.util.*

instructions = IcebergReadInstructions.builder()
    .dataInstructions(s3Instructions)
    .ignoreResolvingErrors(ignoreResolvingErrors)
    .snapshot(snapshot)
    .snapshotId(snapshotId)
    .updateMode(updateMode)
    .build()
```

## Parameters

The following parameters can be set using the builder:

- `s3Instructions`: Instructions for accessing data in S3-compatible storage. Can be an arbitrary object, but is typically an instance of [`io.deephaven.extensions.s3.S3Instructions`](https://docs.deephaven.io/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html).
- `ignoreResolvingErrors`: Controls whether to ignore unexpected resolving errors by silently returning `null` data for columns that can't be resolved.
- `snapshot`: The [`org.apache.iceberg.Snapshot`](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/Snapshot.html) to read. If not specified, the latest snapshot is used.
- `snapshotId`: The ID of the snapshot to read. If not specified, the latest snapshot is used.
- `updateMode`: The [`IcebergUpdateMode`](./iceberg-update-mode.md) to use when reading the table.

## Methods

- [`dataInstructions`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html#dataInstructions()): The data instructions to use for reading Iceberg data files.
- [`ignoreResolvingErrors`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html#ignoreResolvingErrors()): Controls whether to ignore unexpected resolving errors by silently returning `null` data for columns that can't be resolved.
- [`snapshot`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html#snapshot()): The snapshot to load for reading.
- [`snapshotId`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html#snapshotId()): The snapshot ID to load for reading.
- [`updateMode`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html#updateMode()): The [`IcebergUpdateMode`](./iceberg-update-mode.md) to use when reading Iceberg data files.
- [`withSnapshot`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html#withSnapshot(org.apache.iceberg.Snapshot)): Return a copy of the instructions with the snapshot replaced by the specified snapshot.
- [`withSnapshotId`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html#withSnapshotId(long)): Return a copy of the instructions with the snapshot ID replaced by the specified snapshot ID.

## Examples

The following example constructs an `IcebergReadInstructions` object, specifying to ignore resolving errors, read a specific snapshot ID, and that the Iceberg table is static:

```groovy
import io.deephaven.iceberg.util.*

instructions = IcebergReadInstructions.builder()
    .ignoreResolvingErrors(true)
    .snapshotId(1234567890)
    .updateMode(IcebergUpdateMode.staticMode())
    .build()
```

## Related documentation

- [Deephaven and Iceberg](../../../how-to-guides/data-import-export/iceberg.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergUpdateMode`](./iceberg-update-mode.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html)
