---
title: IcebergWriteInstructions
sidebar_label: IcebergWriteInstructions
---

The `IcebergWriteInstructions` class provides instructions intended for writing Deephaven tables as partitions to Iceberg tables.

## Constructors

The `IcebergWriteInstructions` class is constructed using its [builder](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergWriteInstructions.Builder.html):

```groovy syntax
import io.deephaven.iceberg.util.IcebergWriteInstructions

instructions = IcebergWriteInstructions.builder()
    .addAllPartitionPaths(elements)
    .addAllTables(elements)
    .addPartitionPaths(element)
    .addPartitionPaths(elements)
    .addTables(element)
    .addTables(elements)
    .build()
```

- [`addAllPartitionPaths`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergWriteInstructions.Builder.html#addAllPartitionPaths(java.lang.Iterable)): Add all specified partition paths to the write instructions. For this method, `elements` is an iterable of strings representing partition paths.
- [`addAllTables`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergWriteInstructions.Builder.html#addAllTables(java.lang.Iterable)): Add all tables specified in the iterable to the write instructions. For this method, `elements` is an iterable of Deephaven tables.
- [`addPartitionPaths`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergWriteInstructions.Builder.html#addPartitionPaths(java.lang.String)): Add the specified partition path(s) to the write instructions. For this method, `element` is a string representing a partition path, and `elements` is an iterable of strings representing partition paths.
- [`addTables`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergWriteInstructions.Builder.html#addTables(io.deephaven.engine.table.Table)): Add the specified Deephaven table(s) to the write instructions. For this method, `element` is a Deephaven table, and `elements` is an iterable of Deephaven tables.

## Methods

- [`partitionPaths`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergWriteInstructions.html#partitionPaths()): Returns the partition paths where each table will be written.
- [`tables`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergWriteInstructions.html#tables()): Returns the Deephaven tables to be written.

## Examples

The following constructs an `IcebergWriteInstructions` for two tables with identical schemas.

```groovy order=source2024,source2025
import io.deephaven.iceberg.util.*

source2024 = emptyTable(100).update("Year = 2024", "X = i", "Y = 2 * X", "Z = randomDouble(-1, 1)")
source2025 = emptyTable(50).update("Year = 2025", "X = 100 + i", "Y = 3 * X", "Z = randomDouble(-100, 100)")

instructions = IcebergWriteInstructions.builder().addTables(source2024, source2025).build()
```

## Related documentation

- [Deephaven and Iceberg](../../../how-to-guides/data-import-export/iceberg.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergTableWriter`](./iceberg-table-writer.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergWriteInstructions.html)
