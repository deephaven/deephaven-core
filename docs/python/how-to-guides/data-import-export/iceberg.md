---
title: Iceberg and Deephaven
---

[Apache Iceberg](https://iceberg.apache.org/) is a high-performance format for tabular data. Deephaven's Iceberg integration enables users to interact with Iceberg catalogs, namespaces, tables, and snapshots. An Iceberg catalog keeps track of a set of Iceberg tables and groups them into namespaces, much as a database groups its tables into schemas. A snapshot is the state of an Iceberg table at one point in time. Each write to the table creates a new snapshot.

This guide walks through the following tasks:

- Connect to an Iceberg catalog.
- Read an Iceberg table into Deephaven as a static table, as a refreshing table, or as of a specific snapshot.
- Write Deephaven tables to new unpartitioned and partitioned Iceberg tables in the same namespace as the table this guide reads.

The examples in this guide use a [REST catalog](https://iceberg.apache.org/rest-catalog-spec/).

Deephaven's Iceberg integration works with many types of catalogs, including:

- REST
- AWS Glue
- JDBC
- Hive
- Hadoop
- Nessie

> [!NOTE]
> Hive and Nessie catalogs require adding their Iceberg catalog modules (`iceberg-hive-metastore` or `iceberg-nessie`) to your classpath, and a JDBC catalog requires a JDBC driver for its database.

## Deephaven's Iceberg module

The [`deephaven.experimental.iceberg`](/core/pydoc/code/deephaven.experimental.iceberg.html#module-deephaven.experimental.iceberg) module provides Deephaven's Iceberg integration. This guide introduces each Iceberg class and function when an example first uses it. [Related documentation](#related-documentation) links the reference page for each one.

## A Deephaven deployment for Iceberg

The examples in this guide use the Docker deployment found in the [Deephaven example Iceberg REST catalog deployment](https://github.com/deephaven-examples/deephaven-iceberg-rest-catalog/). It includes a Deephaven server, Iceberg REST catalog, MinIO object store, MinIO client, and an Apache Spark server that creates the sample Iceberg table and then shuts down. The repository's README contains a full description of the deployment.

For this guide, you need only clone the repository, `cd` into the `Python` directory, and run `docker compose up`:

```bash
git clone https://github.com/deephaven-examples/deephaven-iceberg-rest-catalog.git
cd deephaven-iceberg-rest-catalog/Python
docker compose up
```

The deployment creates an Iceberg catalog with one table, `taxis`, in the `nyc` namespace. This guide reads that table.

The deployment also uses [application mode](../application-mode.md) to define the connection variables that the examples use: `catalog_uri`, `aws_region`, `aws_access_key_id`, `aws_secret_access_key`, and `s3_endpoint`. Their values come from the deployment's environment variables, so `s3_endpoint` points to the deployment's MinIO server. If you use a different deployment, define these variables yourself before running the examples.

After the deployment starts, open the Deephaven IDE at `http://localhost:10000/ide` to run the examples.

## Interact with the Iceberg catalog

To interact with an Iceberg catalog, you must first create an instance of the [`IcebergCatalogAdapter`](../../reference/iceberg/iceberg-catalog-adapter.md) class. Create one with the [`adapter`](../../reference/iceberg/adapter.md) function, which takes the catalog's configuration properties:

```python docker-config=iceberg test-set=1 order=null
from deephaven.experimental import iceberg

rest_adapter = iceberg.adapter(
    name="generic-adapter",
    properties={
        "type": "rest",
        "uri": catalog_uri,
        "client.region": aws_region,
        "s3.access-key-id": aws_access_key_id,
        "s3.secret-access-key": aws_secret_access_key,
        "s3.endpoint": s3_endpoint,
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    },
)
```

The same function creates adapters for the other catalog types listed above. Set the `type` property to the catalog type, such as `"glue"` for an AWS Glue catalog, and supply the properties that catalog needs.

> [!NOTE]
> If your Iceberg tables are in S3-compatible storage, you can control S3 connectivity with [`S3Instructions`](/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) from the [`deephaven.experimental.s3`](/core/pydoc/code/deephaven.experimental.s3.html#module-deephaven.experimental.s3) module. You can pass them in three places:
>
> - When you create the adapter, pass them to the `s3_instructions` parameter of [`adapter`](../../reference/iceberg/adapter.md).
> - When you read a table, pass them to the `data_instructions` parameter of the table adapter's [`table`](../../reference/iceberg/iceberg-table-adapter.md#methods) method.
> - When you write a table, pass them to the `data_instructions` parameter of [`TableParquetWriterOptions`](../../reference/iceberg/table-parquet-writer-options.md).
>
> If you don't pass them, Deephaven derives the S3 settings from the catalog properties. The examples in this guide don't need them.

The catalog adapter can query the namespaces and tables in a catalog. The following code block calls [`namespaces`](../../reference/iceberg/iceberg-catalog-adapter.md#methods) to get the top-level namespaces and [`tables`](../../reference/iceberg/iceberg-catalog-adapter.md#methods) to get the tables in the `nyc` namespace:

```python docker-config=iceberg test-set=1 order=namespaces,tables
namespaces = rest_adapter.namespaces()
tables = rest_adapter.tables(namespace="nyc")
```

## Read Iceberg tables

### Load an Iceberg table into Deephaven

To load the `nyc.taxis` Iceberg table, call [`load_table`](../../reference/iceberg/iceberg-catalog-adapter.md#methods). Identify the table as `namespace.table`, here `nyc.taxis`. The method returns an [`IcebergTableAdapter`](../../reference/iceberg/iceberg-table-adapter.md) rather than a Deephaven table. The table adapter provides you with several methods to read from or write to the underlying Iceberg table.

```python docker-config=iceberg test-set=1 order=null
iceberg_taxis = rest_adapter.load_table(table_identifier="nyc.taxis")
```

Now that you have the table adapter, read the table into a Deephaven table with [`table`](../../reference/iceberg/iceberg-table-adapter.md#methods). The `update_mode` argument is optional and defaults to [`IcebergUpdateMode.static`](../../reference/iceberg/iceberg-update-mode.md#methods), which loads the table once without refreshing it. [Read a refreshing Iceberg table](#read-a-refreshing-iceberg-table) covers the other modes. The following code sets the static mode explicitly:

```python docker-config=iceberg test-set=1
taxis = iceberg_taxis.table(update_mode=iceberg.IcebergUpdateMode.static())
```

For greater control over the resultant Deephaven table and greater resilience to schema changes, use an [`UnboundResolver`](../../reference/iceberg/unbound-resolver.md) to map the Iceberg table schema to a Deephaven table definition. The following code maps columns by field ID. Iceberg gives every column in a schema a numeric field ID that stays the same when the column is renamed, so the mapping still works after columns are renamed or reordered.

```python docker-config=iceberg test-set=1 order=taxis
from deephaven import dtypes as dht

taxis_def = {
    "VendorID": dht.int64,
    "PickupTime": dht.Instant,
    "DropoffTime": dht.Instant,
    "NumPassengers": dht.double,
    "TripDistance": dht.double,
    "RateCodeID": dht.double,
    "StoreAndFwdFlag": dht.string,
    "PickupLocationID": dht.int64,
    "DropoffLocationID": dht.int64,
    "PaymentType": dht.int64,
    "FareAmount": dht.double,
    "Extra": dht.double,
    "MtaTax": dht.double,
    "Tip": dht.double,
    "Tolls": dht.double,
    "ImprovementSurcharge": dht.double,
    "TotalCost": dht.double,
    "CongestionSurcharge": dht.double,
    "AirportFee": dht.double,
}

column_instructions = {
    "VendorID": 1,
    "PickupTime": 2,
    "DropoffTime": 3,
    "NumPassengers": 4,
    "TripDistance": 5,
    "RateCodeID": 6,
    "StoreAndFwdFlag": 7,
    "PickupLocationID": 8,
    "DropoffLocationID": 9,
    "PaymentType": 10,
    "FareAmount": 11,
    "Extra": 12,
    "MtaTax": 13,
    "Tip": 14,
    "Tolls": 15,
    "ImprovementSurcharge": 16,
    "TotalCost": 17,
    "CongestionSurcharge": 18,
    "AirportFee": 19,
}

resolver_by_id = iceberg.UnboundResolver(
    table_definition=taxis_def, column_instructions=column_instructions
)

iceberg_taxis = rest_adapter.load_table(
    table_identifier="nyc.taxis",
    resolver=resolver_by_id,
)

taxis = iceberg_taxis.table(update_mode=iceberg.IcebergUpdateMode.static())
```

In the `column_instructions` argument of [`UnboundResolver`](../../reference/iceberg/unbound-resolver.md), an `int` value maps a Deephaven column to an Iceberg schema field ID, and a `str` value maps it to a schema field name, such as `{"PickupTime": "tpep_pickup_datetime"}`.

Set the table definition and column names with the resolver when you call [`load_table`](../../reference/iceberg/iceberg-catalog-adapter.md#methods). The arguments to [`table`](../../reference/iceberg/iceberg-table-adapter.md#methods) don't change them.

### Read a refreshing Iceberg table

Deephaven can also read an Iceberg table as a live, refreshing table. When Deephaven loads a new snapshot of the Iceberg table, the Deephaven table updates, and so does every query built on it. The [`IcebergUpdateMode`](../../reference/iceberg/iceberg-update-mode.md) class has three update modes:

- [`static`](../../reference/iceberg/iceberg-update-mode.md#methods): Deephaven loads the Iceberg table once and never refreshes it.
- [`manual_refresh`](../../reference/iceberg/iceberg-update-mode.md#methods): Deephaven loads the table once and refreshes it only when you request a refresh.
- [`auto_refresh`](../../reference/iceberg/iceberg-update-mode.md#methods): Deephaven loads the table once and refreshes it at a set interval, which defaults to once per minute.

The examples above use static Iceberg tables. To refresh a manually refreshing table, call [`update`](../../reference/iceberg/iceberg-table.md#methods) on the [`IcebergTable`](../../reference/iceberg/iceberg-table.md) that [`table`](../../reference/iceberg/iceberg-table-adapter.md#methods) returns. Without an argument, `update` refreshes the table to the latest snapshot. To refresh to a specific snapshot instead, pass its ID. The snapshot must be newer than the one the table currently shows. [Read a specific snapshot](#read-a-specific-snapshot) shows how to list snapshot IDs, and [Refresh a table after a write](#refresh-a-table-after-a-write) shows a manual refresh picking up new data.

To set a refresh interval other than one minute, pass it in milliseconds to the `auto_refresh_ms` argument of [`auto_refresh`](../../reference/iceberg/iceberg-update-mode.md#methods).

The following code block reads the same Iceberg table in the other two modes:

```python docker-config=iceberg test-set=1 ticking-table order=null
# Manually refreshing
taxis_manual = iceberg_taxis.table(
    update_mode=iceberg.IcebergUpdateMode.manual_refresh()
)

# Automatically refreshing every minute
taxis_auto_60s = iceberg_taxis.table(
    update_mode=iceberg.IcebergUpdateMode.auto_refresh()
)

# Automatically refreshing every 30 seconds
taxis_auto_30s = iceberg_taxis.table(
    update_mode=iceberg.IcebergUpdateMode.auto_refresh(auto_refresh_ms=30000)
)
```

### Read a specific snapshot

By default, [`table`](../../reference/iceberg/iceberg-table-adapter.md#methods) reads the most recent snapshot. To read a specific snapshot instead, pass its ID to the `snapshot_id` parameter.

The [`snapshots`](../../reference/iceberg/iceberg-table-adapter.md#methods) method lists the snapshots of an Iceberg table, with each snapshot's ID in the `Id` column and the operation that created it in the `Operation` column.

The deployment empties the `taxis` table before loading data into it, so the table's first snapshot has no rows. The following code reads the table as of that first snapshot, so `taxis_at_snapshot` has the table's columns but no rows:

```python docker-config=iceberg test-set=1 order=taxis_snapshots,taxis_at_snapshot
from deephaven.numpy import to_numpy

taxis_snapshots = iceberg_taxis.snapshots()

first_snapshot_id = int(to_numpy(taxis_snapshots, cols=["Id"])[0][0])
taxis_at_snapshot = iceberg_taxis.table(snapshot_id=first_snapshot_id)
```

## Write Deephaven tables to Iceberg

Deephaven supports only appending data to Iceberg tables. Each append operation creates a new snapshot. A single [`append`](../../reference/iceberg/iceberg-table-writer.md#methods) call that writes multiple tables writes them all in the same snapshot.

To write tables, you create an [`IcebergTableWriter`](../../reference/iceberg/iceberg-table-writer.md). A single writer can write any number of Deephaven tables, as long as each table has the writer's table definition without its partitioning columns:

- For an unpartitioned Iceberg table, each table has the writer's whole table definition.
- For a partitioned Iceberg table, each table leaves out the partitioning columns, and a partition path supplies their values. See [Partitioned Iceberg tables](#partitioned-iceberg-tables).

Each writer belongs to the Iceberg table whose adapter created it. Creating the writer checks its table definition against the Iceberg table's schema, and writes made with it skip that check, so they are faster. Each write still checks that its tables match the writer's table definition and that its partition paths match the table's partitioning.

Because most of the work happens when you create the writer, create one writer per Iceberg table and reuse it for every Deephaven table you append to that table.

To write Deephaven tables to Iceberg, first create the tables you want to write. This example uses two tables:

```python docker-config=iceberg test-set=1 order=source_2024,source_2025
from deephaven import empty_table

source_2024 = empty_table(100).update(
    ["Year = 2024", "X = i", "Y = 2 * X", "Z = randomDouble(-1, 1)"]
)
source_2025 = empty_table(50).update(
    ["Year = 2025", "X = 100 + i", "Y = 3 * X", "Z = randomDouble(-100, 100)"]
)
```

### Unpartitioned Iceberg tables

When writing data to an unpartitioned Iceberg table, you need the Deephaven table definition:

```python docker-config=iceberg test-set=1 order=null
source_def = source_2024.definition
```

Then, create the Iceberg table with [`create_table`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergCatalogAdapter.create_table), which returns an [`IcebergTableAdapter`](../../reference/iceberg/iceberg-table-adapter.md). It takes a table identifier of the form `namespace.table` and a table definition. This example creates the table in the `nyc` namespace, which `create_table` would create if it didn't exist.

`create_table` fails if the table already exists. So that you can run this guide more than once, the following code first drops any existing `nyc.source` table. The Python module has no function for dropping a table, so the code calls the [`dropTable`](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/catalog/Catalog.html#dropTable(org.apache.iceberg.catalog.TableIdentifier,boolean)) method of the Java Iceberg catalog behind the adapter. Its second argument, `True`, also deletes the table's data files. If the table doesn't exist, `dropTable` returns `False` instead of failing.

```python docker-config=iceberg test-set=1 order=null
import jpy

TableIdentifier = jpy.get_type("org.apache.iceberg.catalog.TableIdentifier")

# Drop nyc.source if it exists, so that this example can run more than once
rest_adapter.j_object.catalog().dropTable(TableIdentifier.parse("nyc.source"), True)

source_adapter = rest_adapter.create_table(
    table_identifier="nyc.source", table_definition=source_def
)
```

To create a writer, first define the [`TableParquetWriterOptions`](../../reference/iceberg/table-parquet-writer-options.md) that configure it, then pass them to [`table_writer`](../../reference/iceberg/iceberg-table-adapter.md#methods):

```python docker-config=iceberg test-set=1 order=null
from deephaven.experimental import iceberg

# Define the writer options
writer_options = iceberg.TableParquetWriterOptions(table_definition=source_def)

# Create the writer
source_writer = source_adapter.table_writer(writer_options=writer_options)
```

Now you can write the data to Iceberg. The following code block wraps the `source_2024` and `source_2025` tables in [`IcebergWriteInstructions`](../../reference/iceberg/iceberg-write-instructions.md) and passes the instructions to [`append`](../../reference/iceberg/iceberg-table-writer.md#methods), which writes the tables to the `nyc.source` table:

```python docker-config=iceberg test-set=1 order=null
source_writer.append(iceberg.IcebergWriteInstructions([source_2024, source_2025]))
```

### Partitioned Iceberg tables

To write data to a partitioned Iceberg table, you must build the table definition yourself and mark one or more columns as partitioning columns. Create each column with [`col_def`](/core/pydoc/code/deephaven.column.html#deephaven.column.col_def), and set the `column_type` parameter to [`ColumnType.PARTITIONING`](/core/pydoc/code/deephaven.column.html#deephaven.column.ColumnType) for each partitioning column.

Deephaven can write only to partitioned Iceberg tables whose partition values are plain column values (identity partitioning), with partitioning columns of type boolean, int, long, float, double, `String`, or `LocalDate`. For example, it can't append to `nyc.taxis`, which is partitioned by the day of `tpep_pickup_datetime`. Tables you create with [`create_table`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergCatalogAdapter.create_table) always use identity partitioning.

The following code defines a table with the partitioning column `Year`:

```python docker-config=iceberg test-set=1 order=null
from deephaven.column import col_def, ColumnType
from deephaven import dtypes as dht

source_def_partitioned = [
    col_def("Year", dht.int32, column_type=ColumnType.PARTITIONING),
    col_def("X", dht.int32),
    col_def("Y", dht.int32),
    col_def("Z", dht.double),
]
```

Then, create the partitioned Iceberg table with [`create_table`](/core/pydoc/code/deephaven.experimental.iceberg.html#deephaven.experimental.iceberg.IcebergCatalogAdapter.create_table), which returns an [`IcebergTableAdapter`](../../reference/iceberg/iceberg-table-adapter.md). As before, the code first drops any existing table with the same identifier:

```python docker-config=iceberg test-set=1 order=null
# Drop nyc.source_partitioned if it exists, so that this example can run more than once
rest_adapter.j_object.catalog().dropTable(
    TableIdentifier.parse("nyc.source_partitioned"), True
)

source_adapter_partitioned = rest_adapter.create_table(
    table_identifier="nyc.source_partitioned", table_definition=source_def_partitioned
)
```

As with unpartitioned tables, create a writer with [`TableParquetWriterOptions`](../../reference/iceberg/table-parquet-writer-options.md):

```python docker-config=iceberg test-set=1 order=null
from deephaven.experimental import iceberg

# Define the writer options
writer_options_partitioned = iceberg.TableParquetWriterOptions(
    table_definition=source_def_partitioned
)

# Create the writer
source_writer_partitioned = source_adapter_partitioned.table_writer(
    writer_options=writer_options_partitioned
)
```

Now you can write the data to Iceberg. The `partition_paths` argument of [`IcebergWriteInstructions`](../../reference/iceberg/iceberg-write-instructions.md) takes one partition path per table, in the same order as the tables. A partition path names the partition that a table's rows go to, written as `column=value`, with pairs separated by `/` when the table has more than one partitioning column. In this example, `Year=2024` sends the rows of `source_2024` to the 2024 partition.

Drop the partitioning column from each table before writing, because the partition path supplies its value. The following code block uses [`drop_columns`](../../reference/table-operations/select/drop-columns.md) to remove the `Year` column from the `source_2024` and `source_2025` tables and writes them to the `nyc.source_partitioned` table:

```python docker-config=iceberg test-set=1 order=null
source_writer_partitioned.append(
    iceberg.IcebergWriteInstructions(
        [source_2024.drop_columns("Year"), source_2025.drop_columns("Year")],
        partition_paths=["Year=2024", "Year=2025"],
    )
)
```

### Check the write operations

Check that the writes worked by reading each Iceberg table back into Deephaven with the table adapter that created it:

```python docker-config=iceberg test-set=1 order=source_from_iceberg,source_from_iceberg_partitioned
source_from_iceberg = source_adapter.table()
source_from_iceberg_partitioned = source_adapter_partitioned.table()
```

### Refresh a table after a write

Now that `nyc.source` has data, let's watch a manually refreshing table pick up a new snapshot. The following code block reads `nyc.source` in manual refresh mode and saves a static copy of it with [`snapshot`](../../reference/table-operations/snapshot/snapshot.md). It then appends `source_2025` to `nyc.source` a second time, which creates a new snapshot, and calls [`update`](../../reference/iceberg/iceberg-table.md#methods) to refresh `source_manual` to that snapshot. `source_manual_before` keeps the original 150 rows, while `source_manual` updates to 200 rows:

```python docker-config=iceberg test-set=1 ticking-table order=source_manual_before,source_manual
source_manual = source_adapter.table(
    update_mode=iceberg.IcebergUpdateMode.manual_refresh()
)

# Save a static copy of the table as it is now
source_manual_before = source_manual.snapshot()

# Append source_2025 again, which creates a new snapshot of nyc.source
source_writer.append(iceberg.IcebergWriteInstructions([source_2025]))

# Refresh source_manual to the new snapshot
source_manual.update()
```

Any query you build on `source_manual` updates along with it. With [`auto_refresh`](../../reference/iceberg/iceberg-update-mode.md#methods), Deephaven checks for new snapshots on its own, so your queries keep up with the Iceberg table as other systems write to it.

## Next steps

This guide connected Deephaven to an Iceberg catalog, read an Iceberg table as a static table, a refreshing table, and a past snapshot, and appended Deephaven tables to new Iceberg tables. From here, try the examples against your own catalog, or build real-time queries on refreshing Iceberg tables. The reference pages below cover every option.

## Related documentation

- [`adapter`](../../reference/iceberg/adapter.md)
- [`IcebergCatalogAdapter`](../../reference/iceberg/iceberg-catalog-adapter.md)
- [`IcebergTable`](../../reference/iceberg/iceberg-table.md)
- [`IcebergTableAdapter`](../../reference/iceberg/iceberg-table-adapter.md)
- [`IcebergTableWriter`](../../reference/iceberg/iceberg-table-writer.md)
- [`IcebergUpdateMode`](../../reference/iceberg/iceberg-update-mode.md)
- [`IcebergWriteInstructions`](../../reference/iceberg/iceberg-write-instructions.md)
- [`InferenceResolver`](../../reference/iceberg/inference-resolver.md)
- [`SchemaProvider`](../../reference/iceberg/schema-provider.md)
- [`SortOrderProvider`](../../reference/iceberg/sort-order-provider.md)
- [`TableParquetWriterOptions`](../../reference/iceberg/table-parquet-writer-options.md)
- [`UnboundResolver`](../../reference/iceberg/unbound-resolver.md)
- [Iceberg Pydoc](/core/pydoc/code/deephaven.experimental.iceberg.html)
- [S3 Pydoc](/core/pydoc/code/deephaven.experimental.s3.html)
