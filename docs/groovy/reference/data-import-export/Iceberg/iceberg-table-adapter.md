---
title: IcebergTableAdapter
sidebar_label: IcebergTableAdapter
---

An `IcebergTableAdapter` is a class that manages an Iceberg table and provides methods to interact with it.

## Constructors

The `IcebergTableAdapter` class can be constructed directly:

```groovy syntax
import io.deephaven.iceberg.util.IcebergTableAdapter

adapter = IcebergTableAdapter(
    org.apache.iceberg.catalog.Catalog catalog,
    org.apache.iceberg.catalog.TableIdentifier tableIdentifier,
    org.apache.iceberg.Table table,
    DataInstructionsProviderLoader dataInstructionsProviderLoader,
    Resolver resolver,
    org.apache.iceberg.mapping.NameMapping nameMapping
)
```

It can also be constructed using an [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md) [`load_table`](./iceberg-catalog-adapter.md#methods) method:

```groovy syntax
icebergTableAdapter = icebergCatalogAdapter.load_table(String tableIdentifier)
```

## Parameters

If constructed directly, the parameters are:

- `catalog`: The Iceberg catalog to use.
- `tableIdentifier`: The identifier of the Iceberg table.
- `table`: The Iceberg table.
- `dataInstructionsProviderLoader`: A loader for data instructions.
- `resolver`: A resolver for the table.
- `nameMapping`: The name mapping for the table.

If constructed using [`IcebergCatalogAdapter.load_table`](./iceberg-catalog-adapter.md#methods), the parameter is:

- `tableIdentifier`: The identifier of the Iceberg table to load.

## Methods

- [`catalog`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#catalog()): Return the catalog used to access the Iceberg table.
- [`currentSchema`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#currentSchema()): Retrieve the current schema of the Iceberg table.
- [`currentSnapshot`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#currentSnapshot()): Get the current snapshot of the Iceberg table.
- [`definition`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#definition()): Get the definition of the Iceberg table.
- [`definitionTable`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#definitionTable()): Get a table representation of the Iceberg table definition.
- [`getSnapshot`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#getSnapshot(io.deephaven.iceberg.util.IcebergReadInstructions)): Retrieve a specific snapshot of the Iceberg table by its ID.
- [`icebergTable`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#icebergTable()): Return the underlying Iceberg table.
- [`listSnapshots`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#listSnapshots()): List all snapshots of the Iceberg table.
- [`locationUri`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#locationUri()): Get the location URI of the Iceberg table.
- [`nameMapping`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#nameMapping()): Get the name mapping used for the Iceberg table.
- [`provider`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#provider(io.deephaven.engine.table.impl.locations.TableKey,io.deephaven.iceberg.util.IcebergReadInstructions)): Return the table location provider.
- [`refresh`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#refresh()): Refresh the Iceberg table metadata.
- [`resolver`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#resolver()): Get the resolver for the Iceberg table.
- [`schema`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#schema(int)): Get the identifier of the schema to load.
- [`schemas`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#schemas()): List all schemas of the Iceberg table as a Deephaven table.
- [`snapshots`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#snapshots()): List all snapshots of the Iceberg table as a Deephaven table.
- [`table`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#table()): Read the latest snapshot of the Iceberg table as a Deephaven table.
- [`tableIdentifier`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#tableIdentifier()): Get the identifier of the Iceberg table.
- [`tableWriter`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#tableWriter(io.deephaven.iceberg.util.TableWriterOptions)): Create a new [`IcebergTableWriter`](./iceberg-table-writer.md) for this Iceberg table using the provided options.
- [`toString`](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html#toString()): Get a string representation of the Iceberg table adapter.

## Examples

The following example constructs an [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md) and uses it to create an `IcebergTableAdapter` for a specific table. It uses the Docker deployment defined in the [Deephaven and Iceberg](../../../how-to-guides/data-import-export/iceberg.md) guide.

```groovy docker-config=iceberg order=taxis
import io.deephaven.iceberg.util.*

restAdapter = IcebergTools.createAdapter(
    "minio-iceberg",
    [
        "type": "rest",
        "uri": "http://rest:8181",
        "client.region": "us-east-1",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.endpoint": "http://minio:9000",
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
    ]
)

staticInstructions = IcebergReadInstructions.builder()
    .updateMode(IcebergUpdateMode.staticMode())
    .build()

icebergTaxis = restAdapter.loadTable("nyc.taxis")

taxis = icebergTaxis.table(staticInstructions)
```

## Related documentation

- [Deephaven and Iceberg](../../../how-to-guides/data-import-export/iceberg.md)
- [`IcebergCatalogAdapter`](./iceberg-catalog-adapter.md)
- [`IcebergTableWriter`](./iceberg-table-writer.md)
- [`IcebergReadInstructions`](./iceberg-read-instructions.md)
- [`IcebergUpdateMode`](./iceberg-update-mode.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/iceberg/util/IcebergTableAdapter.html)
