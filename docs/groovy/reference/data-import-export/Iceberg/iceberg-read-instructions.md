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
    .pruningExpression(pruningExpression)
    .snapshot(snapshot)
    .snapshotId(snapshotId)
    .updateMode(updateMode)
    .build()
```

## Parameters

The following parameters can be set using the builder:

- `s3Instructions`: Instructions for accessing data in S3-compatible storage. Can be an arbitrary object, but is typically an instance of [`io.deephaven.extensions.s3.S3Instructions`](https://docs.deephaven.io/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html).
- `ignoreResolvingErrors`: Controls whether to ignore unexpected resolving errors by silently returning `null` data for columns that can't be resolved.
- `pruningExpression`: An [`org.apache.iceberg.expressions.Expression`](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/expressions/Expression.html) that skips Iceberg data files that cannot contain matching rows. Field names resolve against the Iceberg schema rather than against Deephaven column names. The default is `Expressions.alwaysTrue`, which prunes nothing. This parameter prunes; it does not filter. Iceberg prunes using partition values and data file statistics, so a surviving data file is read in full and the result is a superset of the rows that satisfy the expression. To obtain exactly those rows, apply an equivalent Deephaven filter to the result.

  > [!IMPORTANT]
  > Pruning on a non-partition field relies on the per-column value bounds that Iceberg records for each data file. Deephaven's Iceberg writer does not record those statistics, so an expression over a non-partition field prunes nothing on a table that Deephaven wrote. Pruning on a partition field applies in all cases, because Iceberg records partition values regardless of statistics.

- `snapshot`: The [`org.apache.iceberg.Snapshot`](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/Snapshot.html) to read. If not specified, the latest snapshot is used.
- `snapshotId`: The ID of the snapshot to read. If not specified, the latest snapshot is used.
- `updateMode`: The [`IcebergUpdateMode`](./iceberg-update-mode.md) to use when reading the table.

## Methods

- [`dataInstructions`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html#dataInstructions()): The data instructions to use for reading Iceberg data files.
- [`ignoreResolvingErrors`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html#ignoreResolvingErrors()): Controls whether to ignore unexpected resolving errors by silently returning `null` data for columns that can't be resolved.
- [`pruningExpression`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html#pruningExpression()): The Iceberg expression used to skip data files that cannot contain matching rows.
- [`snapshot`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html#snapshot()): The snapshot to load for reading.
- [`snapshotId`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html#snapshotId()): The snapshot ID to load for reading.
- [`updateMode`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html#updateMode()): The [`IcebergUpdateMode`](./iceberg-update-mode.md) to use when reading Iceberg data files.
- [`withPruningExpression`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html#withPruningExpression(org.apache.iceberg.expressions.Expression)): Return a copy of the instructions with the pruning expression replaced by the specified expression.
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

The following example constructs an `IcebergReadInstructions` object that prunes data files whose `region` partition cannot contain the value `EMEA`. Because pruning is not filtering, the query applies an equivalent Deephaven filter to the result to obtain exactly the matching rows:

```groovy
import io.deephaven.iceberg.util.*
import org.apache.iceberg.expressions.Expressions

pruningInstructions = IcebergReadInstructions.builder()
    .pruningExpression(Expressions.equal("region", "EMEA"))
    .build()

// emeaTable = tableAdapter.table(pruningInstructions).where("Region = `EMEA`")
```

Iceberg accepts a Groovy numeric literal directly and widens it to the field's type. A temporal value has no such literal form and must be given as an explicit epoch offset through [`Literal`](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/expressions/Literal.html). The following example prunes on an integer field and on a timestamp field:

```groovy
import io.deephaven.iceberg.util.*
import org.apache.iceberg.expressions.Expression
import org.apache.iceberg.expressions.Expressions

yearInstructions = IcebergReadInstructions.builder()
    .pruningExpression(Expressions.greaterThan("year", 2023))
    .build()

// micros, millis, and nanos state the unit explicitly. Iceberg stores a timestamp column as
// microseconds from the epoch unless the column is timestamp_ns.
timestampInstructions = IcebergReadInstructions.builder()
    .pruningExpression(
        Expressions.predicate(
            Expression.Operation.GT_EQ, "pickup_time", Expressions.micros(1767225600000000L)))
    .build()
```

## Related documentation

- [Deephaven and Iceberg](../../../how-to-guides/data-import-export/iceberg.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`IcebergTableAdapter`](./iceberg-table-adapter.md)
- [`IcebergUpdateMode`](./iceberg-update-mode.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergReadInstructions.html)
